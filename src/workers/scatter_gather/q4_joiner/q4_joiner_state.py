"""Per-client state for the q4_joiner worker: files block half-edges by
(intermediate, a_bucket, b_bucket) on the incoming and outgoing sides, to join
once every Q4Sum shard's EOF has arrived.
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
        # block key (Q4AccountId intermediate, int a_bucket, int b_bucket) -> {endpoint: count}
        self._incoming_by_client: dict[int, dict[tuple, dict]] = {}
        self._outgoing_by_client: dict[int, dict[tuple, dict]] = {}
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
    def eof_change(client_id: int, sender_id: int) -> dict:
        return {"type": "eof", "client_id": client_id, "sender_id": sender_id}

    @staticmethod
    def close_change(client_id: int) -> dict:
        return {"type": "close", "client_id": client_id}

    @staticmethod
    def abort_change(client_id: int) -> dict:
        return Q4JoinerState.close_change(client_id)

    @staticmethod
    def compound_change(*changes: dict) -> dict:
        return {"type": "compound", "changes": list(changes)}

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

    def eof_would_complete(self, client_id: int, sender_id: int) -> bool:
        return self._eof_counter.would_flush(client_id, sender_id)

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

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
        # Single mutation path: runs both live and during WAL replay.
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
        # UpstreamEofCounter ignores duplicates: idempotent on WAL replay.
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
