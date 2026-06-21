"""WorkerState adapter for the q4_filter worker.

Wraps the filter's per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

Change types
------------
  "data"
      A transaction batch arrived. Carries the raw payload (base64) so
      apply_change can replay the source-gate logic identically.

      Source-gate: for each source account, edges accumulate until ≥ 6 distinct
      targets are seen (notebook-bank qualification rule). Once a source qualifies,
      pending edges are emitted as two Q4CountedEdge records each (INCOMING with
      intermediate=target, OUTGOING with intermediate=source), routed to a Q4Sum
      partition by partition_for_parts. Counts per partition are tracked in
      _forwarded_by_partition_by_client so the EOF can tell each Q4Sum shard how
      many records it will receive.

      apply_change replays this entirely in memory (no I/O): updates
      _states_by_client and _forwarded_by_partition_by_client identically to
      the live path, without touching the outbox.

  "close"
      The client was flushed and cleaned up. Drops all per-client maps.
  "coordinator_upstream_eof"
      coordinator.on_upstream_eof was called. action discarded on replay.
      Flush_order mode: updates _seen_eof and, on the leader, _leader_expected.
  "coordinator_msg"
      coordinator.process_control_message was called with a state-mutating type:
      PROCESSED_ANSWER or FLUSH_ACK only. Flush_order mode has no EOF_RECEIVED
      broadcast — each shard's EOF goes directly to the fixed leader via
      on_upstream_eof → SendAnswerAction.
  "coordinator_cleanup"
      coordinator.cleanup_client was called (non-leader, after FLUSH_ORDER).

Coordinator mode: flush_order
------------------------------
Unlike broadcast mode, the flush_order protocol has no EOF_RECEIVED step.
When a shard receives its upstream EOF, it calls on_upstream_eof which on the
non-leader returns SendAnswerAction (send a PROCESSED_ANSWER to the leader
directly). The leader accumulates PROCESSED_ANSWERs until all shards have
reported, then broadcasts FLUSH_ORDER.

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  DATA message
    → data_change(client_id, payload)

  Upstream EOF (data thread):
    action = coordinator.on_upstream_eof(client_id, expected_total, count, fwd)
    → coordinator_upstream_eof_change(client_id, expected_total, count, fwd)
    Execute action outside the lock (SendAnswerAction on non-leader, or
    FlushOrderAction on leader if N=1).

  Response PROCESSED_ANSWER (leader, from a non-leader shard):
    action = coordinator.process_control_message(PROCESSED_ANSWER, client_id, ctrl)
    → coordinator_msg_change(PROCESSED_ANSWER, client_id, ctrl.sender_id,
                              ctrl.expected_total, ctrl.processed_count)
    Execute action outside the lock (FlushOrderAction when last answer arrives).

  Non-leader close (FLUSH_ORDER received → FlushAction(is_leader=False)):
    Read forwarded_by_partition(client_id) → build per-partition EOF batch.
    coordinator.cleanup_client(client_id)
    → coordinator_cleanup_change(client_id) + close_change(client_id)
      Bundle into a single compound change (one change per message).

  Leader close (all FLUSH_ACKs → FlushAction(is_leader=True)):
    action = coordinator.process_control_message(FLUSH_ACK, client_id, ctrl)
    → coordinator_msg_change(FLUSH_ACK, client_id, ...)
    When FlushAction(is_leader=True): emit downstream gateway EOF.
    → close_change(client_id)
    (_on_flush_ack already cleaned coordinator state; no coordinator_cleanup needed.)

  N=1 shortcut (on_upstream_eof returns FlushAction directly):
    → coordinator_upstream_eof_change(...)
    Emit per-partition EOF using forwarded_by_partition(client_id).
    → close_change(client_id)

State accessors (read before close_change)
------------------------------------------
  forwarded_by_partition(client_id) → dict[int, int]  per-partition send counts
  source_states_for(client_id)      → dict  per-source qualification state
  processed_count(client_id)        → int
  forwarded_total(client_id)        → int  sum of all partition counts
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field

from common.bank_ids import notebook_bank_id
from common.eof_coordinator import EofCoordinator
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal import (
    Q4AccountId,
    Q4TransactionEdge,
    TransactionSerializer,
    partition_for_parts,
)


_tx_ser = TransactionSerializer()

# Number of distinct targets a source must reach before it qualifies.
# Matches the threshold in _accept_edge_locked in filters.py.
_QUALIFY_TARGETS = 6


@dataclass
class _SourceState:
    """Per-source accumulator for the source-gate. Mirrors filters._SourceState."""
    # set[Q4AccountId] — Q4AccountId is a frozen dataclass (hashable, picklable)
    targets: set = field(default_factory=set)
    qualified: bool = False
    # list[Q4TransactionEdge] — pending edges waiting for qualification
    pending: list = field(default_factory=list)


class Q4FilterState:
    def __init__(self, coordinator: EofCoordinator, sum_amount: int) -> None:
        # coordinator is shared with the worker; snapshotted here so that
        # EOF broadcast / flush protocol state survives a restart.
        self._coordinator = coordinator
        # sum_amount determines partition routing for forwarded counted edges.
        self._sum_amount = sum_amount
        # source Q4AccountId → _SourceState
        self._states_by_client: dict[int, dict] = {}
        # partition index → forwarded count; needed to emit per-partition EOF totals.
        self._forwarded_by_partition_by_client: dict[int, dict[int, int]] = {}
        self._processed_by_client: dict[int, int] = {}
        self._closed_by_client: set[int] = set()

    # ---------- change constructors ----------

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
        # flush_order mode: relevant types are PROCESSED_ANSWER and FLUSH_ACK.
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

    # ---------- state accessors (read before close_change) ----------

    def source_states_for(self, client_id: int) -> dict:
        """Per-source qualification state; live worker reads this at close time."""
        return self._states_by_client.get(client_id, {})

    def forwarded_by_partition(self, client_id: int) -> dict[int, int]:
        """Counts sent to each Q4Sum partition; used to build the per-partition EOF."""
        return self._forwarded_by_partition_by_client.get(client_id, {})

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def forwarded_total(self, client_id: int) -> int:
        return sum(self._forwarded_by_partition_by_client.get(client_id, {}).values())

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        # _SourceState dataclasses are picklable; Q4AccountId / Q4TransactionEdge
        # are frozen dataclasses with primitive fields — all picklable too.
        return {
            "states_by_client": {
                cid: dict(src_states)
                for cid, src_states in self._states_by_client.items()
            },
            "forwarded_by_partition_by_client": {
                cid: dict(counts)
                for cid, counts in self._forwarded_by_partition_by_client.items()
            },
            "processed_by_client": dict(self._processed_by_client),
            "eof_coordinator": self._coordinator.snapshot(),
            "closed_by_client": set(self._closed_by_client),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._states_by_client = {}
            self._forwarded_by_partition_by_client = {}
            self._processed_by_client = {}
            self._closed_by_client = set()
            return
        self._states_by_client = {
            cid: dict(src_states)
            for cid, src_states in data["states_by_client"].items()
        }
        self._forwarded_by_partition_by_client = {
            cid: dict(counts)
            for cid, counts in data["forwarded_by_partition_by_client"].items()
        }
        self._processed_by_client = dict(data["processed_by_client"])
        self._coordinator.restore(data["eof_coordinator"])
        self._closed_by_client = set(data["closed_by_client"])

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

    # ---------- private ----------

    def _apply_data(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        payload = base64.b64decode(change["payload_b64"])
        transactions = _tx_ser.deserialize_batch(payload)
        src_states = self._states_by_client.setdefault(client_id, {})
        forwarded = self._forwarded_by_partition_by_client.setdefault(client_id, {})
        for tx in transactions:
            edge = Q4TransactionEdge(
                source=Q4AccountId(
                    bank_id=notebook_bank_id(tx.from_bank),
                    account=(tx.from_account or "").strip(),
                ),
                target=Q4AccountId(
                    bank_id=notebook_bank_id(tx.to_bank),
                    account=(tx.to_account or "").strip(),
                ),
            )
            self._accept_edge(src_states, forwarded, edge)
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + len(transactions)
        )

    def _accept_edge(self, src_states: dict, forwarded: dict, edge: Q4TransactionEdge) -> None:
        """Mirrors _accept_edge_locked from filters.py — state mutation only, no I/O."""
        state = src_states.get(edge.source)
        if state is None:
            state = _SourceState()
            src_states[edge.source] = state

        if state.qualified:
            self._count_qualified_edge(forwarded, edge)
            return

        state.pending.append(edge)
        if len(state.targets) < _QUALIFY_TARGETS:
            state.targets.add(edge.target)
        if len(state.targets) < _QUALIFY_TARGETS:
            return

        # Source just qualified — emit all pending edges.
        state.qualified = True
        state.targets.clear()
        for e in state.pending:
            self._count_qualified_edge(forwarded, e)
        state.pending = []

    def _count_qualified_edge(self, forwarded: dict, edge: Q4TransactionEdge) -> None:
        """Update forwarded counts for both counted edges emitted by _emit_qualified_edge.

        _emit_qualified_edge sends two Q4CountedEdge records per Q4TransactionEdge:
          - Q4_EDGE_INCOMING: intermediate = edge.target → routed to partition of target
          - Q4_EDGE_OUTGOING: intermediate = edge.source → routed to partition of source
        """
        p_in = partition_for_parts(
            (edge.target.bank_id, edge.target.account), self._sum_amount
        )
        forwarded[p_in] = forwarded.get(p_in, 0) + 1
        p_out = partition_for_parts(
            (edge.source.bank_id, edge.source.account), self._sum_amount
        )
        forwarded[p_out] = forwarded.get(p_out, 0) + 1

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: popping a missing key is a no-op.
        self._states_by_client.pop(client_id, None)
        self._forwarded_by_partition_by_client.pop(client_id, None)
        self._processed_by_client.pop(client_id, None)
        self._closed_by_client.add(client_id)
