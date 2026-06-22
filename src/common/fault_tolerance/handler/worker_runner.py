"""Reusable consume loop around PersistentStateHandler.

The handler owns disk and memory; it does not own the RabbitMQ channel. This
runner is the worker-agnostic glue that drives it for the **data plane**:

  startup : recover() then re-publish the outbox (so nothing confirmed-but-not-
            committed is lost across a crash).
  message : unpack the addressed packet, run it through handle(), and act on the
            returned instruction — publish the outputs (publisher confirms make
            send() block until the broker acks), then commit_done(), then ack.

A failure anywhere nacks with requeue=True so RabbitMQ redelivers to the same
personal queue; recovery then sees the input as APPLIED/NEW and is idempotent.

The runner shares the worker's lock so that callers driving other consumers
(e.g. an EOF coordinator on its own threads) serialize with handle()/commit.
Publishing happens outside the lock: it is network I/O and the handler state it
needs is already captured in the returned instruction.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import (
    PersistentStateHandler,
)
from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.message_protocol.internal.protocol import InternalProtocol

# Worker business logic: given the input's client_id and payload, return the
# state change and the logical (destination, body) outputs. The destination is a
# logical name resolved against the publisher registry.
ProcessPayload = Callable[[int, bytes], "tuple[dict, list[tuple[str, bytes]]]"]


class WorkerRunner:
    def __init__(
        self,
        handler: PersistentStateHandler,
        publishers: dict[str, object],
        process_payload: ProcessPayload,
        lock: threading.Lock,
    ) -> None:
        self._handler = handler
        self._publishers = publishers
        self._process_payload = process_payload
        self._lock = lock

    def recover_and_republish(self) -> None:
        with self._lock:
            self._handler.recover()
            pending = self._handler.outbox_to_republish()
        self._publish(pending)
        logging.info("worker_runner_recovered | republished=%s", len(pending))

    def process(self, body: bytes, ack: Callable, nack: Callable) -> None:
        try:
            msg_type, client_id, sender_id, seq, payload = (
                InternalProtocol.unpack_addressed_packet(body)
            )
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

    def _publish(self, entries: list[OutboxEntry]) -> None:
        for entry in entries:
            publisher = self._publishers.get(entry.destination)
            if publisher is None:
                raise KeyError(f"no publisher for destination {entry.destination!r}")
            if entry.shard is None:
                publisher.send(entry.body)
            else:
                publisher.send_to_shard(entry.body, entry.shard)
