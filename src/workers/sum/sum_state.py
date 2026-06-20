"""WorkerState adapter for the sum worker.

Wraps the sum worker's mutable per-client state behind the snapshot/restore/
apply_change contract the durable-state engine expects.

Change types
------------
  "data"
      A DATA batch arrived. The raw payload is base64-encoded so apply_change
      can replay deserialization + processor.process() identically. The actual
      partials forwarded to aggregators live in the durable outbox.
  "close"
      The client was flushed and cleaned up. Processor and counter are dropped;
      late DATA messages for that client are ignored on replay.
  "coordinator_upstream_eof"
      coordinator.on_upstream_eof was called. action discarded on replay.
  "coordinator_msg"
      coordinator.process_control_message was called with a state-mutating type:
      EOF_RECEIVED, PROCESSED_ANSWER, or FLUSH_ACK. FLUSH_ACK uses the standard
      path — _on_flush_ack handles coordinator leader-state cleanup internally.
  "coordinator_cleanup"
      coordinator.cleanup_client was called (non-leader, after FLUSH_ORDER).

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  DATA message
    → data_change(client_id, payload)

  Upstream EOF (data thread):
    action = coordinator.on_upstream_eof(client_id, expected_total, count, fwd)
    → coordinator_upstream_eof_change(client_id, expected_total, count, fwd)
    Execute action outside the lock.

  Control EOF_RECEIVED (broadcast, control thread):
    action = coordinator.process_control_message(EOF_RECEIVED, client_id, ctrl, count, fwd)
    → coordinator_msg_change(EOF_RECEIVED, client_id, ctrl.sender_id,
                              ctrl.expected_total, ctrl.processed_count, count, fwd)
    Execute action outside the lock.

  Response PROCESSED_ANSWER (leader):
    action = coordinator.process_control_message(PROCESSED_ANSWER, client_id, ctrl)
    → coordinator_msg_change(PROCESSED_ANSWER, client_id, ctrl.sender_id,
                              ctrl.expected_total, ctrl.processed_count)
    Execute action outside the lock.

  Non-leader close (FLUSH_ORDER → FlushAction(is_leader=False)):
    Bundle coordinator_cleanup_change + close_change into a single compound
    change dict, because handle() supports one change per message. Call order
    in apply_change: coordinator.cleanup_client → _apply_close.
    Read partials_for(client_id) → emit to outbox before closing.

  Leader close (final FLUSH_ACK → FlushAction(is_leader=True), or N=1):
    action = coordinator.process_control_message(FLUSH_ACK, client_id, ctrl)
    → coordinator_msg_change(FLUSH_ACK, client_id, ...)
    Read partials_for(client_id) → emit to outbox.
    → close_change(client_id)

  N=1 shortcut:
    → coordinator_upstream_eof_change(...)
    Read partials_for(client_id) → emit.
    → close_change(client_id)

Note: _partials_forwarded is a global logging counter with no per-client
semantics — intentionally omitted from state.
"""

from __future__ import annotations

import base64
from collections.abc import Callable

from common.eof_coordinator import EofCoordinator
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.transaction_serializer import TransactionSerializer

ProcessorFactory = Callable[[], object]

_tx_ser = TransactionSerializer()


class SumState:
    def __init__(
        self,
        coordinator: EofCoordinator,
        processor_factory: ProcessorFactory,
    ) -> None:
        # coordinator is shared with the worker and snapshotted alongside the
        # per-client maps so that EOF progress survives a crash.
        self._coordinator = coordinator
        # Factory is called once per client on first DATA message; it must return
        # the right processor type for this sum worker's CONFIGURATION.
        self._processor_factory = processor_factory
        self._processed_by_client: dict[int, int] = {}
        self._processors_by_client: dict = {}

    @staticmethod
    def data_change(client_id: int, payload: bytes) -> dict:
        # payload is base64 because the WAL frames state_change dicts as JSON.
        return {
            "type": "data",
            "client_id": client_id,
            "payload_b64": base64.b64encode(payload).decode("ascii"),
        }

    @staticmethod
    def close_change(client_id: int) -> dict:
        return {"type": "close", "client_id": client_id}

    @staticmethod
    def coordinator_upstream_eof_change(
        client_id: int, expected_total: int, count: int, forwarded: int
    ) -> dict:
        return {
            "type": "coordinator_upstream_eof",
            "client_id": client_id,
            "expected_total": expected_total,
            "count": count,
            "forwarded": forwarded,
        }

    @staticmethod
    def coordinator_msg_change(
        msg_type: MessageType,
        client_id: int,
        sender_id: int,
        expected_total: int,
        processed_count: int,
        count: int = 0,
        forwarded: int = 0,
    ) -> dict:
        return {
            "type": "coordinator_msg",
            "msg_type": msg_type.value,
            "client_id": client_id,
            "sender_id": sender_id,
            "expected_total": expected_total,
            "processed_count": processed_count,
            "count": count,
            "forwarded": forwarded,
        }

    @staticmethod
    def coordinator_cleanup_change(client_id: int) -> dict:
        # Non-leader cleanup after FlushAction(is_leader=False).
        return {"type": "coordinator_cleanup", "client_id": client_id}

    def partials_for(self, client_id: int) -> list[tuple[str, bytes]]:
        """Accumulated partials to forward at flush time; read before close_change."""
        processor = self._processors_by_client.get(client_id)
        return processor.partials() if processor is not None else []

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        # SumProcessor subclasses hold only dicts and primitives — picklable.
        return {
            "processed_by_client": dict(self._processed_by_client),
            "processors_by_client": dict(self._processors_by_client),
            "eof_coordinator": self._coordinator.snapshot(),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._processed_by_client = {}
            self._processors_by_client = {}
            return
        self._processed_by_client = dict(data["processed_by_client"])
        self._processors_by_client = dict(data["processors_by_client"])
        self._coordinator.restore(data["eof_coordinator"])

    def apply_change(self, change: dict) -> None:
        # Single mutation path — runs both live and during WAL replay.
        kind = change["type"]
        client_id = change["client_id"]
        if kind == "data":
            self._apply_data(client_id, change)
        elif kind == "close":
            self._apply_close(client_id)
        elif kind == "coordinator_upstream_eof":
            self._coordinator.on_upstream_eof(
                client_id, change["expected_total"], change["count"], change["forwarded"]
            )
        elif kind == "coordinator_msg":
            ctrl = ControlMessage(
                sender_id=change["sender_id"],
                expected_total=change["expected_total"],
                processed_count=change["processed_count"],
            )
            self._coordinator.process_control_message(
                MessageType(change["msg_type"]),
                client_id, ctrl, change["count"], change["forwarded"],
            )
        elif kind == "coordinator_cleanup":
            self._coordinator.cleanup_client(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    def _apply_data(self, client_id: int, change: dict) -> None:
        # Unlike AggregatorState (whose processor.accept() takes raw bytes),
        # SumProcessor.process() takes Transaction objects — deserialization
        # must happen here before passing to the processor.
        payload = base64.b64decode(change["payload_b64"])
        transactions = _tx_ser.deserialize_batch(payload)
        processor = self._processor_for(client_id)
        for transaction in transactions:
            processor.process(transaction)
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + len(transactions)
        )

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: re-closing an already-closed client is a no-op.
        self._processors_by_client.pop(client_id, None)
        self._processed_by_client.pop(client_id, None)

    def _processor_for(self, client_id: int):
        return self._processors_by_client.setdefault(
            client_id, self._processor_factory()
        )
