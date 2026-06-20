"""WorkerState adapter for the q4_aggregator worker.

Wraps the aggregator's per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

Three kinds of change:
  - "data": a Q4PairPaths batch arrived. The change carries the raw payload
    (base64) so apply_change can replay the pair counting + qualification logic
    and keep _pair_counts_by_client / _qualified_pairs_by_client consistent.
    When a pair's running total reaches Q4_QUALIFY_THRESHOLD, it moves from
    counts to qualified; the actual account-candidate emissions go to the durable
    outbox.
  - "eof": one upstream joiner sent its EOF. The change carries sender_id;
    apply_change advances the UpstreamEofCounter (idempotent: duplicate
    sender_ids are silently ignored).
  - "close": the client was fully emitted and cleaned up. apply_change pops all
    per-client maps, closes the EOF counter entry, and marks the client closed.

_forwarded_by_partition_by_client is intentionally omitted: those counts are
computed fresh during the emit phase at close time and are not needed for any
decision between message arrivals.
"""

from __future__ import annotations

import base64

from common.message_protocol.internal import (
    Q4AccountId,
    Q4PairPathsSerializer,
    Q4_QUALIFY_THRESHOLD,
)
from common.upstream_eof_counter import UpstreamEofCounter


_pair_ser = Q4PairPathsSerializer()


class Q4AggregatorState:
    def __init__(self, joiner_amount: int) -> None:
        self._eof_counter = UpstreamEofCounter(joiner_amount)
        # (src_bank, src_acc, tgt_bank, tgt_acc) → running path_count total
        self._pair_counts_by_client: dict[int, dict[tuple, int]] = {}
        # pairs whose total reached Q4_QUALIFY_THRESHOLD
        self._qualified_pairs_by_client: dict[int, set[tuple]] = {}
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

    def pair_counts_for(self, client_id: int) -> dict[tuple, int]:
        return self._pair_counts_by_client.get(client_id, {})

    def qualified_pairs_for(self, client_id: int) -> set[tuple]:
        return self._qualified_pairs_by_client.get(client_id, set())

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def eof_count(self, client_id: int) -> int:
        return self._eof_counter.count(client_id)

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        # Tuple keys survive pickle without conversion.
        return {
            "pair_counts_by_client": {
                cid: dict(counts)
                for cid, counts in self._pair_counts_by_client.items()
            },
            "qualified_pairs_by_client": {
                cid: set(pairs)
                for cid, pairs in self._qualified_pairs_by_client.items()
            },
            "processed_by_client": dict(self._processed_by_client),
            "eof_counter": self._eof_counter.snapshot(),
            "closed_by_client": set(self._closed_by_client),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._pair_counts_by_client = {}
            self._qualified_pairs_by_client = {}
            self._processed_by_client = {}
            self._closed_by_client = set()
            return
        self._pair_counts_by_client = {
            cid: dict(counts)
            for cid, counts in data["pair_counts_by_client"].items()
        }
        self._qualified_pairs_by_client = {
            cid: set(pairs)
            for cid, pairs in data["qualified_pairs_by_client"].items()
        }
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
        batch = _pair_ser.deserialize_batch(payload)
        counts = self._pair_counts_by_client.setdefault(client_id, {})
        qualified = self._qualified_pairs_by_client.setdefault(client_id, set())
        for pair_paths in batch:
            if pair_paths.source == pair_paths.target:
                continue
            key = _pair_key(pair_paths.source, pair_paths.target)
            if key in qualified:
                continue
            total = min(
                Q4_QUALIFY_THRESHOLD,
                counts.get(key, 0) + int(pair_paths.path_count),
            )
            if total >= Q4_QUALIFY_THRESHOLD:
                counts.pop(key, None)
                qualified.add(key)
            else:
                counts[key] = total
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + len(batch)
        )

    def _apply_eof(self, client_id: int, change: dict) -> None:
        # UpstreamEofCounter ignores duplicates — idempotent on WAL replay.
        self._eof_counter.on_eof(client_id, change["sender_id"])

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: popping a missing key is a no-op.
        self._pair_counts_by_client.pop(client_id, None)
        self._qualified_pairs_by_client.pop(client_id, None)
        self._processed_by_client.pop(client_id, None)
        self._eof_counter.close(client_id)
        self._closed_by_client.add(client_id)


def _pair_key(source: Q4AccountId, target: Q4AccountId) -> tuple:
    return (source.bank_id, source.account, target.bank_id, target.account)
