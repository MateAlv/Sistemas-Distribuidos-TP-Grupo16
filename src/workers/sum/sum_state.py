"""WorkerState adapter for the sum worker.

Wraps the sum worker's mutable per-client state behind the snapshot/restore/
apply_change contract the durable-state engine expects.

Five kinds of change:
  - "data": a DATA batch was processed. The change carries the raw batch payload
    (base64) so apply_change can replay deserialization + processor.process() and
    keep the accumulated partial results consistent with what the live worker
    produced. The actual partials forwarded to aggregators live in the durable
    outbox.
  - "close": the client was flushed and cleaned up. apply_change drops the
    processor and counter; subsequent DATA messages for that client are ignored.

  The following three changes keep the EofCoordinator's internal state in the WAL
  so that crash recovery does not rely solely on the last snapshot:

  - "coordinator_upstream_eof": coordinator.on_upstream_eof was called. apply_change
    replays the call (action discarded — no I/O on replay).
  - "coordinator_msg": coordinator.process_control_message was called with a
    state-mutating type (EOF_RECEIVED, PROCESSED_ANSWER, or FLUSH_ACK). apply_change
    replays the call (action discarded). FLUSH_ACK uses the standard path — the
    coordinator's _on_flush_ack handles leader-state cleanup internally.
  - "coordinator_cleanup": coordinator.cleanup_client was called (non-leader path).
    apply_change replays it.

Note: _partials_forwarded is a global logging counter that is not per-client and
does not affect business-logic outputs — intentionally omitted from state.
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
