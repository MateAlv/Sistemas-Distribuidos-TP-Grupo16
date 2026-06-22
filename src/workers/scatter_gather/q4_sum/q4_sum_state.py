"""WorkerState adapter for the q4_sum worker.

Wraps the sum worker's per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

Change types
------------
  "data"
      A Q4CountedEdge batch arrived. Carries the raw payload (base64);
      apply_change deserializes and accumulates each edge's count into
      _incoming_by_client or _outgoing_by_client, keyed by the intermediate
      Q4AccountId. Mirrors the live worker's _accept_counted_edges without I/O.
  "eof"
      One upstream Q4Filter shard sent its EOF. Carries sender_id; apply_change
      advances the UpstreamEofCounter. Idempotent: duplicates are ignored.
  "close"
      The client was fully emitted and cleaned up. Drops all per-client maps,
      closes the EOF counter entry, and marks closed so late messages are
      ignored on WAL replay.

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  DATA message
    → data_change(client_id, payload)

  EOF from one upstream Q4Filter shard:
    → eof_change(client_id, sender_id)
    If eof_count(client_id) == filter_amount after applying:
      Read incoming_for(client_id) + outgoing_for(client_id).
      Emit block-join edge batches to outbox (per partition to Q4Joiner).
      → close_change(client_id)

State accessors (read before close_change)
------------------------------------------
  incoming_for(client_id) → dict  intermediate → {endpoint: count} (INCOMING)
  outgoing_for(client_id) → dict  intermediate → {endpoint: count} (OUTGOING)
  processed_count(client_id) → int
  eof_count(client_id)        → int  how many Q4Filter shards have sent EOF so far

Note: _forwarded_by_partition_by_client is intentionally omitted — computed fresh
at emit time (_emit_client_blocks) and not needed between arrivals.
"""

from __future__ import annotations

import base64

from common.message_protocol.internal import (
    Q4CountedEdgeSerializer,
    Q4_EDGE_INCOMING,
    Q4_EDGE_OUTGOING,
)
from common.upstream_eof_counter import UpstreamEofCounter


_counted_edge_ser = Q4CountedEdgeSerializer()


class Q4SumState:
    def __init__(self, filter_amount: int) -> None:
        self._eof_counter = UpstreamEofCounter(filter_amount)
        # intermediate Q4AccountId → endpoint Q4AccountId → accumulated count
        self._incoming_by_client: dict[int, dict] = {}
        self._outgoing_by_client: dict[int, dict] = {}
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
    def eof_change(client_id: int, sender_id: int) -> dict:
        return {"type": "eof", "client_id": client_id, "sender_id": sender_id}

    @staticmethod
    def close_change(client_id: int) -> dict:
        return {"type": "close", "client_id": client_id}

    @staticmethod
    def compound_change(*changes: dict) -> dict:
        return {"type": "compound", "changes": list(changes)}

    # ---------- state accessors (read before close_change) ----------

    def incoming_for(self, client_id: int) -> dict:
        """Accumulated INCOMING counts per intermediate; read at emit time."""
        return self._incoming_by_client.get(client_id, {})

    def outgoing_for(self, client_id: int) -> dict:
        """Accumulated OUTGOING counts per intermediate; read at emit time."""
        return self._outgoing_by_client.get(client_id, {})

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def eof_count(self, client_id: int) -> int:
        return self._eof_counter.count(client_id)

    def eof_would_complete(self, client_id: int, sender_id: int) -> bool:
        """Read-only: would this EOF be the last upstream shard (triggering flush)?
        Used by the decide step before mutating via eof_change."""
        return self._eof_counter.would_flush(client_id, sender_id)

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        # Q4AccountId frozen dataclasses are picklable and hashable (used as dict keys).
        return {
            "incoming_by_client": _copy_edge_map(self._incoming_by_client),
            "outgoing_by_client": _copy_edge_map(self._outgoing_by_client),
            "processed_by_client": dict(self._processed_by_client),
            "eof_counter": self._eof_counter.snapshot(),
            "closed_by_client": set(self._closed_by_client),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._incoming_by_client = {}
            self._outgoing_by_client = {}
            self._processed_by_client = {}
            self._closed_by_client = set()
            return
        self._incoming_by_client = _copy_edge_map(data["incoming_by_client"])
        self._outgoing_by_client = _copy_edge_map(data["outgoing_by_client"])
        self._processed_by_client = dict(data["processed_by_client"])
        self._eof_counter.restore(data["eof_counter"])
        self._closed_by_client = set(data["closed_by_client"])

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
        elif kind == "eof":
            self._apply_eof(client_id, change)
        elif kind == "close":
            self._apply_close(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    # ---------- private ----------

    def _apply_data(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        payload = base64.b64decode(change["payload_b64"])
        edges = _counted_edge_ser.deserialize_batch(payload)
        incoming = self._incoming_by_client.setdefault(client_id, {})
        outgoing = self._outgoing_by_client.setdefault(client_id, {})
        for edge in edges:
            if edge.role == Q4_EDGE_INCOMING:
                endpoint_counts = incoming.setdefault(edge.intermediate, {})
            elif edge.role == Q4_EDGE_OUTGOING:
                endpoint_counts = outgoing.setdefault(edge.intermediate, {})
            else:
                raise ValueError(f"unexpected counted edge role: {edge.role}")
            endpoint_counts[edge.endpoint] = (
                endpoint_counts.get(edge.endpoint, 0) + edge.count
            )
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + len(edges)
        )

    def _apply_eof(self, client_id: int, change: dict) -> None:
        # UpstreamEofCounter ignores duplicates — idempotent on WAL replay.
        self._eof_counter.on_eof(client_id, change["sender_id"])

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: popping a missing key is a no-op.
        self._incoming_by_client.pop(client_id, None)
        self._outgoing_by_client.pop(client_id, None)
        self._processed_by_client.pop(client_id, None)
        self._eof_counter.close(client_id)
        self._closed_by_client.add(client_id)


def _copy_edge_map(by_client: dict) -> dict:
    """Shallow-copy two levels: {client_id: {intermediate: {endpoint: count}}}."""
    return {cid: {k: dict(v) for k, v in edges.items()} for cid, edges in by_client.items()}
