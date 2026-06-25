"""Per-client state for the q4_filter worker.

Gates each source account: its edges wait until the source has reached 6 distinct
targets, then qualify and forward as counted edges to Q4Sum partitions.
predict_data_outputs runs the gate on a copy so its outputs match apply_change.
"""

from __future__ import annotations

import base64
from collections import defaultdict
from dataclasses import dataclass, field

from common.bank_ids import notebook_bank_id
from common.eof_coordinator import EofCoordinator
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal import (
    Q4CountedEdge,
    Q4_EDGE_INCOMING,
    Q4_EDGE_OUTGOING,
    Q4AccountId,
    Q4TransactionEdge,
    TransactionSerializer,
    partition_for_parts,
)


_tx_ser = TransactionSerializer()

# Distinct targets a source must reach before it qualifies (scatter-gather rule).
_QUALIFY_TARGETS = 6


@dataclass
class _SourceState:
    """Per-source accumulator for the source-gate."""
    # set[Q4AccountId]; frozen dataclass, so hashable and picklable for snapshots
    targets: set = field(default_factory=set)
    qualified: bool = False
    # pending edges waiting for the source to qualify
    pending: list = field(default_factory=list)


class Q4FilterState:
    def __init__(self, coordinator: EofCoordinator, sum_amount: int) -> None:
        # coordinator is shared with the worker; snapshotted here so the
        # flush protocol state survives a restart.
        self._coordinator = coordinator
        # sum_amount determines partition routing for forwarded counted edges.
        self._sum_amount = sum_amount
        # client_id -> {source Q4AccountId: _SourceState}
        self._states_by_client: dict[int, dict] = {}
        # client_id -> {partition index: forwarded count}, for per-partition EOF totals
        self._forwarded_by_partition_by_client: dict[int, dict[int, int]] = {}
        self._processed_by_client: dict[int, int] = {}
        self._closed_by_client: set[int] = set()

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
    def abort_change(client_id: int) -> dict:
        return Q4FilterState.compound_change(
            Q4FilterState.coordinator_cleanup_change(client_id),
            Q4FilterState.close_change(client_id),
        )

    @staticmethod
    def coordinator_cleanup_change(client_id: int) -> dict:
        # Non-leader cleanup after FlushAction(is_leader=False).
        return {"type": "coordinator_cleanup", "client_id": client_id}

    @staticmethod
    def compound_change(*changes: dict) -> dict:
        return {"type": "compound", "changes": list(changes)}

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

    def predict_data_outputs(self, client_id: int, payload: bytes) -> dict[int, list]:
        """Read-only: the counted edges this batch would emit, grouped by Q4Sum
        partition, computed on per-source copies so they match what
        apply_change(data_change) will produce."""
        if client_id in self._closed_by_client:
            return {}
        transactions = _tx_ser.deserialize_batch(payload)
        src_states = self._states_by_client.get(client_id, {})
        work: dict = {}
        by_partition: dict[int, list] = defaultdict(list)
        for tx in transactions:
            edge = self._edge_from_tx(tx)
            state = work.get(edge.source)
            if state is None:
                state = _copy_source_state(src_states.get(edge.source))
                work[edge.source] = state
            for counted in self._gate_emit(state, edge):
                by_partition[self._partition_of(counted)].append(counted)
        return dict(by_partition)

    def snapshot(self) -> dict:
        # _SourceState, Q4AccountId and Q4TransactionEdge are all picklable.
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
        # Single mutation path: runs both live and during WAL replay.
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
        payload = base64.b64decode(change["payload_b64"])
        transactions = _tx_ser.deserialize_batch(payload)
        src_states = self._states_by_client.setdefault(client_id, {})
        forwarded = self._forwarded_by_partition_by_client.setdefault(client_id, {})
        for tx in transactions:
            edge = self._edge_from_tx(tx)
            state = src_states.get(edge.source)
            if state is None:
                state = _SourceState()
                src_states[edge.source] = state
            for counted in self._gate_emit(state, edge):
                partition = self._partition_of(counted)
                forwarded[partition] = forwarded.get(partition, 0) + 1
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + len(transactions)
        )

    @staticmethod
    def _edge_from_tx(tx) -> Q4TransactionEdge:
        return Q4TransactionEdge(
            source=Q4AccountId(
                bank_id=notebook_bank_id(tx.from_bank),
                account=(tx.from_account or "").strip(),
            ),
            target=Q4AccountId(
                bank_id=notebook_bank_id(tx.to_bank),
                account=(tx.to_account or "").strip(),
            ),
        )

    def _gate_emit(self, state: "_SourceState", edge: Q4TransactionEdge) -> list:
        """Source-gate step shared by apply (real state) and predict (a copy).
        Returns the two Q4CountedEdge records for a qualified edge (INCOMING at
        target, OUTGOING at source), or none while the source is below threshold."""
        if state.qualified:
            return _counted_for(edge)
        state.pending.append(edge)
        if len(state.targets) < _QUALIFY_TARGETS:
            state.targets.add(edge.target)
        if len(state.targets) < _QUALIFY_TARGETS:
            return []
        # Source just qualified: flush all pending edges.
        state.qualified = True
        state.targets.clear()
        emitted: list = []
        for pending_edge in state.pending:
            emitted.extend(_counted_for(pending_edge))
        state.pending = []
        return emitted

    def _partition_of(self, counted: Q4CountedEdge) -> int:
        return partition_for_parts(
            (counted.intermediate.bank_id, counted.intermediate.account),
            self._sum_amount,
        )

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: popping a missing key is a no-op.
        self._states_by_client.pop(client_id, None)
        self._forwarded_by_partition_by_client.pop(client_id, None)
        self._processed_by_client.pop(client_id, None)
        self._closed_by_client.add(client_id)


def _counted_for(edge: Q4TransactionEdge) -> list:
    return [
        Q4CountedEdge(
            role=Q4_EDGE_INCOMING, intermediate=edge.target, endpoint=edge.source, count=1
        ),
        Q4CountedEdge(
            role=Q4_EDGE_OUTGOING, intermediate=edge.source, endpoint=edge.target, count=1
        ),
    ]


def _copy_source_state(state: "_SourceState | None") -> "_SourceState":
    """Shallow copy for the predictor: targets and pending are copied (edges are
    frozen, safe to share) so predict can run the gate without touching real state."""
    if state is None:
        return _SourceState()
    return _SourceState(
        targets=set(state.targets), qualified=state.qualified, pending=list(state.pending)
    )
