"""WorkerState adapter for the q4_joiner worker.

Wraps the joiner's per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

Three kinds of change:
  - "data": a Q4BlockJoinEdge batch arrived. The change carries the raw payload
    (base64); apply_change deserializes the batch and files each half-edge into
    _incoming_by_client or _outgoing_by_client by (intermediate, a_bucket,
    b_bucket) block key, accumulating endpoint→count sums. This mirrors the live
    worker's _accept_block_edges without any I/O.
  - "eof": one upstream sum worker sent its EOF. The change carries sender_id;
    apply_change advances the UpstreamEofCounter.
  - "close": the client was fully emitted and cleaned up. apply_change drops all
    per-client maps, closes the EOF counter entry, and marks the client closed.

_forwarded_by_partition_by_client is intentionally omitted: those counts are
computed fresh during the emit phase (_emit_client_pairs) at close time and
are not needed for any decision between message arrivals.
"""

from __future__ import annotations

import base64

from common.message_protocol.internal import (
    Q4BlockJoinEdgeSerializer,
    Q4_EDGE_INCOMING,
    Q4_EDGE_OUTGOING,
)
from common.upstream_eof_counter import UpstreamEofCounter


_block_edge_ser = Q4BlockJoinEdgeSerializer()


class Q4JoinerState:
    def __init__(self, sum_amount: int) -> None:
        self._eof_counter = UpstreamEofCounter(sum_amount)
        # block key → endpoint → count
        # block key: (Q4AccountId intermediate, int a_bucket, int b_bucket)
        self._incoming_by_client: dict[int, dict[tuple, dict]] = {}
        self._outgoing_by_client: dict[int, dict[tuple, dict]] = {}
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

    # ---------- state accessors (read before close_change) ----------

    def incoming_for(self, client_id: int) -> dict[tuple, dict]:
        """Half-edges on the INCOMING side; keyed by (intermediate, a, b) block."""
        return self._incoming_by_client.get(client_id, {})

    def outgoing_for(self, client_id: int) -> dict[tuple, dict]:
        """Half-edges on the OUTGOING side; keyed by (intermediate, a, b) block."""
        return self._outgoing_by_client.get(client_id, {})

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def eof_count(self, client_id: int) -> int:
        return self._eof_counter.count(client_id)

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        # Q4AccountId frozen dataclasses are picklable; tuple block keys survive too.
        return {
            "incoming_by_client": _copy_nested(self._incoming_by_client),
            "outgoing_by_client": _copy_nested(self._outgoing_by_client),
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
        self._incoming_by_client = _copy_nested(data["incoming_by_client"])
        self._outgoing_by_client = _copy_nested(data["outgoing_by_client"])
        self._processed_by_client = dict(data["processed_by_client"])
        self._eof_counter.restore(data["eof_counter"])
        self._closed_by_client = set(data["closed_by_client"])

    def apply_change(self, change: dict) -> None:
        # Single mutation path — runs both live and during WAL replay.
        kind = change["type"]
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
        edges = _block_edge_ser.deserialize_batch(payload)
        incoming = self._incoming_by_client.setdefault(client_id, {})
        outgoing = self._outgoing_by_client.setdefault(client_id, {})
        for edge in edges:
            block = (edge.intermediate, edge.a_bucket, edge.b_bucket)
            if edge.role == Q4_EDGE_INCOMING:
                counts = incoming.setdefault(block, {})
            elif edge.role == Q4_EDGE_OUTGOING:
                counts = outgoing.setdefault(block, {})
            else:
                raise ValueError(f"unexpected block edge role: {edge.role}")
            counts[edge.endpoint] = counts.get(edge.endpoint, 0) + edge.count
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


def _copy_nested(by_client: dict) -> dict:
    """Shallow-copy two levels deep: {client_id: {block_key: {endpoint: count}}}."""
    return {cid: {k: dict(v) for k, v in blocks.items()} for cid, blocks in by_client.items()}
