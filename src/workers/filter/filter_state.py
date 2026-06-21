"""WorkerState adapter for the filter worker.

Wraps the filter's mutable per-client counters and EofCoordinator state behind
the snapshot/restore/apply_change contract the durable-state engine expects.

Change types
------------
  "data"
      A DATA batch was processed. Carries only the resulting counts (processed
      and forwarded per output), NOT the raw payload. apply_change updates
      accumulators without re-running the filter — outputs live in the outbox.
  "flush_ack"
      A FLUSH_ACK arrived from a non-leader peer (N>1). Records sender_id and
      its per-output forwarded counts so the leader can detect when all
      FILTER_AMOUNT-1 peers have acked. Idempotent: duplicate sender_ids are
      absorbed by set membership.
  "close"
      The client was fully flushed and cleaned up. Pops all per-client state
      and marks closed. Idempotent on replay.
  "coordinator_upstream_eof"
      coordinator.on_upstream_eof was called. action discarded on replay.
  "coordinator_msg"
      coordinator.process_control_message was called with a state-mutating type:
      EOF_RECEIVED or PROCESSED_ANSWER only. FLUSH_ACK is NOT routed here —
      this worker receives FLUSH_ACK with a custom payload (forwarded_by_output
      dict) and calls coordinator.cleanup_leader_state instead of the standard
      FLUSH_ACK path.
  "coordinator_cleanup"
      coordinator.cleanup_client (is_leader=False, non-leader path) or
      coordinator.cleanup_leader_state (is_leader=True, leader path) was called.
      Discriminated by the "is_leader" bool in the change dict.

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  DATA message
    processed_n, fwd_by_output = filter_batch(payload)
    → data_change(client_id, processed_n, fwd_by_output)

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

  Custom FLUSH_ACK received (leader, from a non-leader peer):
    → flush_ack_change(client_id, sender_id, forwarded_by_output)
    If flushed_ack_count(client_id) == FILTER_AMOUNT - 1 (last ack):
      emit downstream EOF using all_forwarded_by_output(client_id).
      coordinator.cleanup_leader_state(client_id)
      → coordinator_cleanup_change(client_id, is_leader=True) + close_change(client_id)
        Bundle these two into a single compound change (one change per message).

  Non-leader close (leader broadcasts FLUSH_ORDER → worker flushes → sends FLUSH_ACK):
    Read forwarded_by_output(client_id) → build FLUSH_ACK payload.
    coordinator.cleanup_client(client_id)
    → coordinator_cleanup_change(client_id, is_leader=False) + close_change(client_id)
      Bundle these two into a single compound change (one change per message).

  N=1 shortcut (no FLUSH_ACK protocol):
    → coordinator_upstream_eof_change(...)
    Emit downstream EOF.
    → close_change(client_id)

Fields omitted from state (logging-only, non-deterministic):
  first_data_logged_by_client, deserialized_by_client.
"""

from __future__ import annotations

from common.eof_coordinator import EofCoordinator
from common.message_protocol.internal.common import ControlMessage, MessageType


class FilterState:
    def __init__(self, coordinator: EofCoordinator) -> None:
        # coordinator is shared with the worker and snapshotted alongside the
        # per-client maps so that EOF progress survives a crash.
        self._coordinator = coordinator
        self._processed_by_client: dict[int, int] = {}
        self._forwarded_by_client: dict[int, int] = {}
        self._forwarded_by_output_by_client: dict[int, dict[str, int]] = {}
        self._closed_by_client: set[int] = set()
        # The two fields below are only populated in the N>1 case: the leader
        # accumulates FLUSH_ACKs from all non-leader peers before forwarding EOF.
        self._all_forwarded_by_output_by_client: dict[int, dict[str, int]] = {}
        self._flushed_acks_by_client: dict[int, set[int]] = {}

    @staticmethod
    def data_change(
        client_id: int,
        processed_count: int,
        forwarded_by_output: dict[str, int],
    ) -> dict:
        # forwarded_by_output is the incremental per-output count for this batch
        # (not cumulative); apply_change accumulates it into the running total.
        return {
            "type": "data",
            "client_id": client_id,
            "processed_count": processed_count,
            "forwarded_by_output": forwarded_by_output,
        }

    @staticmethod
    def flush_ack_change(
        client_id: int,
        sender_id: int,
        forwarded_by_output: dict[str, int],
    ) -> dict:
        return {
            "type": "flush_ack",
            "client_id": client_id,
            "sender_id": sender_id,
            "forwarded_by_output": forwarded_by_output,
        }

    @staticmethod
    def close_change(client_id: int) -> dict:
        return {"type": "close", "client_id": client_id}

    @staticmethod
    def coordinator_upstream_eof_change(
        client_id: int, expected_total: int, count: int, forwarded: int
    ) -> dict:
        # Records the on_upstream_eof call so WAL replay can reproduce the
        # _leader_expected update inside the coordinator.
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
        # Records a state-mutating process_control_message call (EOF_RECEIVED or
        # PROCESSED_ANSWER). FLUSH_ACK uses cleanup_leader_state instead.
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
    def coordinator_cleanup_change(client_id: int, is_leader: bool = False) -> dict:
        # is_leader=True  → coordinator.cleanup_leader_state (leader custom-FLUSH_ACK path)
        # is_leader=False → coordinator.cleanup_client (non-leader after FlushAction)
        return {
            "type": "coordinator_cleanup",
            "client_id": client_id,
            "is_leader": is_leader,
        }

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def forwarded_by_output(self, client_id: int) -> dict[str, int]:
        return dict(self._forwarded_by_output_by_client.get(client_id, {}))

    def forwarded_count(self, client_id: int) -> int:
        return self._forwarded_by_client.get(client_id, 0)

    def all_forwarded_by_output(self, client_id: int) -> dict[str, int]:
        return dict(self._all_forwarded_by_output_by_client.get(client_id, {}))

    def flushed_ack_count(self, client_id: int) -> int:
        return len(self._flushed_acks_by_client.get(client_id, set()))

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        return {
            "processed_by_client": dict(self._processed_by_client),
            "forwarded_by_client": dict(self._forwarded_by_client),
            "forwarded_by_output_by_client": {
                cid: dict(by_output)
                for cid, by_output in self._forwarded_by_output_by_client.items()
            },
            "closed_by_client": set(self._closed_by_client),
            "all_forwarded_by_output_by_client": {
                cid: dict(by_output)
                for cid, by_output in self._all_forwarded_by_output_by_client.items()
            },
            "flushed_acks_by_client": {
                cid: set(senders)
                for cid, senders in self._flushed_acks_by_client.items()
            },
            "eof_coordinator": self._coordinator.snapshot(),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._processed_by_client = {}
            self._forwarded_by_client = {}
            self._forwarded_by_output_by_client = {}
            self._closed_by_client = set()
            self._all_forwarded_by_output_by_client = {}
            self._flushed_acks_by_client = {}
            return
        self._processed_by_client = dict(data["processed_by_client"])
        self._forwarded_by_client = dict(data["forwarded_by_client"])
        self._forwarded_by_output_by_client = {
            cid: dict(by_output)
            for cid, by_output in data["forwarded_by_output_by_client"].items()
        }
        self._closed_by_client = set(data["closed_by_client"])
        self._all_forwarded_by_output_by_client = {
            cid: dict(by_output)
            for cid, by_output in data["all_forwarded_by_output_by_client"].items()
        }
        self._flushed_acks_by_client = {
            cid: set(senders)
            for cid, senders in data["flushed_acks_by_client"].items()
        }
        self._coordinator.restore(data["eof_coordinator"])

    def apply_change(self, change: dict) -> None:
        # Single mutation path — runs both live and during WAL replay.
        kind = change["type"]
        client_id = change["client_id"]
        if kind == "data":
            self._apply_data(client_id, change)
        elif kind == "flush_ack":
            self._apply_flush_ack(client_id, change)
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
            if change["is_leader"]:
                self._coordinator.cleanup_leader_state(client_id)
            else:
                self._coordinator.cleanup_client(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    def _apply_data(self, client_id: int, change: dict) -> None:
        # Closed clients are ignored; late DATA messages after a crash-recovery
        # cycle may replay against an already-closed client.
        if client_id in self._closed_by_client:
            return
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + change["processed_count"]
        )
        by_output = change["forwarded_by_output"]
        forwarded_total = sum(by_output.values())
        self._forwarded_by_client[client_id] = (
            self._forwarded_by_client.get(client_id, 0) + forwarded_total
        )
        acc = self._forwarded_by_output_by_client.setdefault(client_id, {})
        for output, count in by_output.items():
            acc[output] = acc.get(output, 0) + count

    def _apply_flush_ack(self, client_id: int, change: dict) -> None:
        # Accumulate a non-leader's counts; idempotent if the same sender_id is
        # replayed (set membership prevents double-counting).
        sender_id = change["sender_id"]
        acks = self._flushed_acks_by_client.setdefault(client_id, set())
        if sender_id in acks:
            return
        acks.add(sender_id)
        agg = self._all_forwarded_by_output_by_client.setdefault(client_id, {})
        for output, count in change["forwarded_by_output"].items():
            agg[output] = agg.get(output, 0) + count

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: re-closing an already-closed client is a no-op.
        self._processed_by_client.pop(client_id, None)
        self._forwarded_by_client.pop(client_id, None)
        self._forwarded_by_output_by_client.pop(client_id, None)
        self._all_forwarded_by_output_by_client.pop(client_id, None)
        self._flushed_acks_by_client.pop(client_id, None)
        self._closed_by_client.add(client_id)
