"""WorkerState adapter for the filter_q5_usd worker.

Wraps the worker's per-client processed/forwarded counters and the EofCoordinator
behind the snapshot/restore/apply_change contract the durable-state engine expects.

Change types
------------
  "data"
      A DATA batch was processed. Carries only the resulting counts (processed
      and forwarded), NOT the raw payload or USD conversion results. apply_change
      updates the accumulators without re-running the filter — outputs live in
      the durable outbox.
  "close"
      The client was fully flushed and cleaned up. Both per-client counters are
      dropped; late DATA messages are ignored.
  "coordinator_upstream_eof"
      coordinator.on_upstream_eof was called. action discarded on replay.
  "coordinator_msg"
      coordinator.process_control_message was called with a state-mutating type:
      EOF_RECEIVED, PROCESSED_ANSWER, or FLUSH_ACK. FLUSH_ACK uses the standard
      path — _on_flush_ack handles leader-state cleanup internally.
  "coordinator_cleanup"
      coordinator.cleanup_client was called (non-leader, after FLUSH_ORDER).

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  DATA message
    processed_n, forwarded_n = filter_batch(payload)
    → data_change(client_id, processed_n, forwarded_n)

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
    change dict (handle() supports one change per message). Call order in
    apply_change: coordinator.cleanup_client → _apply_close.

  Leader close (final FLUSH_ACK → FlushAction(is_leader=True), or N=1):
    action = coordinator.process_control_message(FLUSH_ACK, client_id, ctrl)
    → coordinator_msg_change(FLUSH_ACK, client_id, ...)
    Emit downstream EOF (total processed_count + forwarded_count to outbox).
    → close_change(client_id)

  N=1 shortcut:
    → coordinator_upstream_eof_change(...)
    Emit downstream EOF.
    → close_change(client_id)

Note: rates_loaded is excluded from state — the rates manager is re-initialised
from the RPC service on every worker startup.
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
        self._agg_seq_by_client: dict[int, int] = {}
        self._closed_by_client: set[int] = set()

    @staticmethod
    def data_change(
        client_id: int, processed_count: int, forwarded_count: int, seq_advance: int = 0
    ) -> dict:
        return {
            "type": "data",
            "client_id": client_id,
            "processed_count": processed_count,
            "forwarded_count": forwarded_count,
            "seq_advance": seq_advance,
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

    @staticmethod
    def compound_change(*changes: dict) -> dict:
        # Bundle multiple changes so PersistentStateHandler sees one change per handle().
        return {"type": "compound", "changes": list(changes)}

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def forwarded_count(self, client_id: int) -> int:
        return self._forwarded_by_client.get(client_id, 0)

    def agg_seq(self, client_id: int) -> int:
        """Current durable outgoing seq for addressed packets to aggregators."""
        return self._agg_seq_by_client.get(client_id, 0)

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        return {
            "processed_by_client": dict(self._processed_by_client),
            "forwarded_by_client": dict(self._forwarded_by_client),
            "agg_seq_by_client": dict(self._agg_seq_by_client),
            "closed_by_client": list(self._closed_by_client),
            "eof_coordinator": self._coordinator.snapshot(),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._processed_by_client = {}
            self._forwarded_by_client = {}
            self._agg_seq_by_client = {}
            self._closed_by_client = set()
            return
        self._processed_by_client = {int(k): v for k, v in data["processed_by_client"].items()}
        self._forwarded_by_client = {int(k): v for k, v in data["forwarded_by_client"].items()}
        self._agg_seq_by_client = {int(k): v for k, v in data["agg_seq_by_client"].items()}
        self._closed_by_client = set(data["closed_by_client"])
        self._coordinator.restore(data["eof_coordinator"])

    def apply_change(self, change: dict) -> None:
        # Single mutation path — runs both live and during WAL replay.
        kind = change["type"]
        if kind == "compound":
            for sub in change["changes"]:
                self.apply_change(sub)
            return
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
        if client_id in self._closed_by_client:
            return
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + change["processed_count"]
        )
        if change["forwarded_count"]:
            self._forwarded_by_client[client_id] = (
                self._forwarded_by_client.get(client_id, 0) + change["forwarded_count"]
            )
        if change.get("seq_advance"):
            self._agg_seq_by_client[client_id] = (
                self._agg_seq_by_client.get(client_id, 0) + change["seq_advance"]
            ) & 0xFFFFFFFF

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: popping a missing key is a no-op.
        self._closed_by_client.add(client_id)
        self._processed_by_client.pop(client_id, None)
        self._forwarded_by_client.pop(client_id, None)
        self._agg_seq_by_client.pop(client_id, None)
