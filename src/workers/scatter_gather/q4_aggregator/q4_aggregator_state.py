"""WorkerState adapter for the q4_aggregator worker.

Wraps the aggregator's per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

DATA changes replay pair counting and qualification. When a pair reaches
Q4_QUALIFY_THRESHOLD it is marked qualified and the two account-candidate counts
are recorded by deduper partition. The actual account-candidate packets are
created by the worker and stored in the durable outbox.
"""

from __future__ import annotations

import base64
from collections import defaultdict

from common.message_protocol.internal import (
    Q4AccountId,
    Q4PairPathsSerializer,
    Q4_QUALIFY_THRESHOLD,
    partition_for_parts,
)
from common.upstream_eof_counter import UpstreamEofCounter


_pair_ser = Q4PairPathsSerializer()


class Q4AggregatorState:
    def __init__(self, joiner_amount: int, deduper_amount: int) -> None:
        self._eof_counter = UpstreamEofCounter(joiner_amount)
        self._deduper_amount = deduper_amount
        self._pair_counts_by_client: dict[int, dict[tuple, int]] = {}
        self._qualified_pairs_by_client: dict[int, set[tuple]] = {}
        self._forwarded_by_partition_by_client: dict[int, dict[int, int]] = {}
        self._processed_by_client: dict[int, int] = {}
        self._closed_by_client: set[int] = set()

    # ---------- change constructors ----------

    @staticmethod
    def data_change(client_id: int, payload: bytes) -> dict:
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

    # ---------- state accessors ----------

    def pair_counts_for(self, client_id: int) -> dict[tuple, int]:
        return self._pair_counts_by_client.get(client_id, {})

    def qualified_pairs_for(self, client_id: int) -> set[tuple]:
        return self._qualified_pairs_by_client.get(client_id, set())

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def eof_count(self, client_id: int) -> int:
        return self._eof_counter.count(client_id)

    def eof_would_complete(self, client_id: int, sender_id: int) -> bool:
        return self._eof_counter.would_flush(client_id, sender_id)

    def forwarded_by_partition(self, client_id: int) -> dict[int, int]:
        return self._forwarded_by_partition_by_client.get(client_id, {})

    def forwarded_total(self, client_id: int) -> int:
        return sum(self._forwarded_by_partition_by_client.get(client_id, {}).values())

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    def predict_data_outputs(self, client_id: int, payload: bytes) -> dict[int, list]:
        """Return account candidates this batch would emit, without mutation."""
        if client_id in self._closed_by_client:
            return {}
        batch = _pair_ser.deserialize_batch(payload)
        counts = dict(self._pair_counts_by_client.get(client_id, {}))
        qualified = set(self._qualified_pairs_by_client.get(client_id, set()))
        by_partition: dict[int, list] = defaultdict(list)
        _apply_batch(
            batch,
            counts,
            qualified,
            lambda key: _append_pair_accounts(
                by_partition,
                key,
                self._deduper_amount,
            ),
        )
        return dict(by_partition)

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        return {
            "pair_counts_by_client": {
                cid: dict(counts)
                for cid, counts in self._pair_counts_by_client.items()
            },
            "qualified_pairs_by_client": {
                cid: set(pairs)
                for cid, pairs in self._qualified_pairs_by_client.items()
            },
            "forwarded_by_partition_by_client": {
                cid: dict(counts)
                for cid, counts in self._forwarded_by_partition_by_client.items()
            },
            "processed_by_client": dict(self._processed_by_client),
            "eof_counter": self._eof_counter.snapshot(),
            "closed_by_client": set(self._closed_by_client),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._pair_counts_by_client = {}
            self._qualified_pairs_by_client = {}
            self._forwarded_by_partition_by_client = {}
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
        self._forwarded_by_partition_by_client = {
            cid: dict(counts)
            for cid, counts in data.get("forwarded_by_partition_by_client", {}).items()
        }
        self._processed_by_client = dict(data["processed_by_client"])
        self._eof_counter.restore(data["eof_counter"])
        self._closed_by_client = set(data["closed_by_client"])

    def apply_change(self, change: dict) -> None:
        kind = change["type"]
        if kind == "compound":
            for sub in change["changes"]:
                self.apply_change(sub)
            return
        client_id = change["client_id"]
        if kind == "data":
            self._apply_data(client_id, change)
        elif kind == "eof":
            self._eof_counter.on_eof(client_id, change["sender_id"])
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
        forwarded = self._forwarded_by_partition_by_client.setdefault(client_id, {})

        def on_qualified(key) -> None:
            for account in (_source_from_key(key), _target_from_key(key)):
                partition = _account_partition(account, self._deduper_amount)
                forwarded[partition] = forwarded.get(partition, 0) + 1

        _apply_batch(batch, counts, qualified, on_qualified)
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + len(batch)
        )

    def _apply_close(self, client_id: int) -> None:
        self._pair_counts_by_client.pop(client_id, None)
        self._qualified_pairs_by_client.pop(client_id, None)
        self._forwarded_by_partition_by_client.pop(client_id, None)
        self._processed_by_client.pop(client_id, None)
        self._eof_counter.close(client_id)
        self._closed_by_client.add(client_id)


def _apply_batch(batch, counts: dict, qualified: set, on_qualified) -> None:
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
            on_qualified(key)
        else:
            counts[key] = total


def _append_pair_accounts(
    by_partition: dict[int, list],
    key: tuple,
    deduper_amount: int,
) -> None:
    for account in (_source_from_key(key), _target_from_key(key)):
        by_partition[_account_partition(account, deduper_amount)].append(account)


def _pair_key(source: Q4AccountId, target: Q4AccountId) -> tuple:
    return (source.bank_id, source.account, target.bank_id, target.account)


def _source_from_key(key) -> Q4AccountId:
    return Q4AccountId(bank_id=key[0], account=key[1])


def _target_from_key(key) -> Q4AccountId:
    return Q4AccountId(bank_id=key[2], account=key[3])


def _account_partition(account: Q4AccountId, deduper_amount: int) -> int:
    return partition_for_parts(
        (account.bank_id, account.account),
        deduper_amount,
    )
