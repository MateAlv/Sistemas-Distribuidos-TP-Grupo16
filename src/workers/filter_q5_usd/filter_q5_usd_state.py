"""WorkerState adapter for the filter_q5_usd worker.

Wraps the worker's per-client processed/forwarded counters and EOF coordinator
behind the snapshot/restore/apply_change contract the durable-state engine expects.

Five kinds of change:
  - "data": a DATA batch was processed. The change carries only the resulting
    counts (processed and forwarded), NOT the raw payload or the USD conversion
    results. apply_change updates the accumulators without re-running the filter;
    the actual transaction outputs live in the durable outbox.
  - "close": the client was fully flushed and cleaned up. apply_change pops both
    per-client counters (mirroring the pop in _handle_upstream_eof / _handle_control
    after the FlushAction).

  The following three changes keep the EofCoordinator's internal state in the WAL
  so that crash recovery does not rely solely on the last snapshot:

  - "coordinator_upstream_eof": coordinator.on_upstream_eof was called. apply_change
    replays the call (action discarded — no I/O on replay).
  - "coordinator_msg": coordinator.process_control_message was called with a
    state-mutating type (EOF_RECEIVED, PROCESSED_ANSWER, or FLUSH_ACK). apply_change
    replays the call (action discarded). FLUSH_ACK is the standard path here — the
    coordinator's _on_flush_ack cleans up leader state internally when all acks arrive.
  - "coordinator_cleanup": coordinator.cleanup_client was called (non-leader, after
    FlushAction(is_leader=False)). apply_change replays it.

Note: rates_loaded is intentionally excluded from state — the rates manager is
re-initialised from the RPC service on every worker startup.
"""

from __future__ import annotations

from common.eof_coordinator import EofCoordinator
from common.message_protocol.internal.common import ControlMessage, MessageType


class FilterQ5UsdState:
    def __init__(self, coordinator: EofCoordinator) -> None:
        # coordinator is shared with the worker and snapshotted here so that
        # upstream-EOF progress (broadcast / flush protocol state) survives a crash.
        self._coordinator = coordinator
        self._processed_by_client: dict[int, int] = {}
        self._forwarded_by_client: dict[int, int] = {}

    @staticmethod
    def data_change(client_id: int, processed_count: int, forwarded_count: int) -> dict:
        return {
            "type": "data",
            "client_id": client_id,
            "processed_count": processed_count,
            "forwarded_count": forwarded_count,
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

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def forwarded_count(self, client_id: int) -> int:
        return self._forwarded_by_client.get(client_id, 0)

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        return {
            "processed_by_client": dict(self._processed_by_client),
            "forwarded_by_client": dict(self._forwarded_by_client),
            "eof_coordinator": self._coordinator.snapshot(),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._processed_by_client = {}
            self._forwarded_by_client = {}
            return
        self._processed_by_client = dict(data["processed_by_client"])
        self._forwarded_by_client = dict(data["forwarded_by_client"])
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
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + change["processed_count"]
        )
        if change["forwarded_count"]:
            self._forwarded_by_client[client_id] = (
                self._forwarded_by_client.get(client_id, 0) + change["forwarded_count"]
            )

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: popping a missing key is a no-op.
        self._processed_by_client.pop(client_id, None)
        self._forwarded_by_client.pop(client_id, None)
