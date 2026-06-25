"""Consume loop around PersistentStateHandler for the data plane.

At startup it recovers and republishes the outbox so nothing confirmed but not
committed is lost across a crash. Per message it unpacks the addressed packet,
runs handle(), then publishes the outputs, commit_done()s and acks. Any failure
nacks with requeue=True; redelivery is safe because recovery is idempotent.

The runner shares the worker's lock so callers driving other consumers (e.g. an
EOF coordinator) serialize with handle()/commit. Publishing stays outside the
lock since it is network I/O and the instruction already carries what it needs.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import (
    PersistentStateHandler,
)
from common.fault_tolerance.inbox.msg_kind import MsgKind
from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.message_protocol.internal.common.message_type import MessageType
from common.message_protocol.internal.protocol import InternalProtocol

# Worker business logic: given the input's client_id and payload, return the
# state change and the logical (destination, body) outputs. The destination is a
# logical name resolved against the publisher registry.
ProcessPayload = Callable[[int, bytes], "tuple[dict, list[tuple[str, bytes]]]"]
# Abort business logic: given the aborted client_id, return the state change and
# the downstream ABORT outputs to propagate. Return (change, []) for terminal
# workers that have no downstream to notify.
AbortFn = Callable[[int], "tuple[dict, list]"]


class WorkerRunner:
    def __init__(
        self,
        handler: PersistentStateHandler,
        publishers: dict[str, object],
        process_payload: ProcessPayload,
        lock: threading.Lock,
        abort_fn: AbortFn | None = None,
    ) -> None:
        self._handler = handler
        self._publishers = publishers
        self._process_payload = process_payload
        self._lock = lock
        self._abort_fn = abort_fn

    def recover_and_republish(self) -> None:
        with self._lock:
            self._handler.recover()
            pending = self._handler.outbox_to_republish()
        self._publish(pending)
        logging.info(
            "worker_runner_recovered | node=%s | republished_outputs=%s | "
            "message=back online after restart; durable state replayed and "
            "pending outputs re-published",
            self._handler.node_id,
            len(pending),
        )

    def process(self, body: bytes, ack: Callable, nack: Callable) -> None:
        try:
            msg_type, client_id, sender_id, seq, payload = (
                InternalProtocol.unpack_addressed_packet(body)
            )

            if msg_type == MessageType.ABORT:
                self._handle_abort(client_id, sender_id, seq, ack, nack)
                return

            msg_id = f"{sender_id}:{seq}"
            business_fn = lambda data: self._process_payload(client_id, data)

            with self._lock:
                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, payload, business_fn
                )

            if instruction.action is Action.PUBLISH_THEN_COMMIT:
                self._publish(instruction.outputs)
                with self._lock:
                    self._handler.commit_done(*instruction.ctx)

            ack()
        except Exception as e:
            logging.error("worker_runner_message_error | error=%s", e, exc_info=True)
            nack(requeue=True)

    def _handle_abort(
        self, client_id: int, sender_id: int, seq: int, ack: Callable, nack: Callable
    ) -> None:
        logging.info(
            "worker_runner_abort_received | node=%s | client_id=%s | "
            "message=client disconnect signal received; flushing per-client state",
            self._handler.node_id,
            client_id,
        )
        if self._abort_fn is None:
            logging.warning("worker_runner_abort_unhandled | client_id=%s", client_id)
            nack(requeue=False)
            return
        try:
            msg_id = f"abort:{sender_id}:{client_id}:{seq}"
            with self._lock:
                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, b"",
                    lambda _: self._abort_fn(client_id),
                    kind=MsgKind.ABORT,
                )
            if instruction.action is Action.PUBLISH_THEN_COMMIT:
                self._publish(instruction.outputs)
                with self._lock:
                    self._handler.commit_done(*instruction.ctx)
            ack()
        except Exception as e:
            logging.error("worker_runner_abort_error | error=%s", e, exc_info=True)
            nack(requeue=True)

    def _publish(self, entries: list[OutboxEntry]) -> None:
        for entry in entries:
            publisher = self._publishers.get(entry.destination)
            if publisher is None:
                raise KeyError(f"no publisher for destination {entry.destination!r}")
            if entry.shard is None:
                publisher.send(entry.body)
            else:
                publisher.send_to_shard(entry.body, entry.shard)
