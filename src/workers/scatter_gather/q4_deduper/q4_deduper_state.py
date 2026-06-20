"""WorkerState adapter for the q4_deduper worker.

Wraps the deduper's per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

Change types
------------
  "data"
      A Q4AccountId batch arrived. Carries the raw payload (base64);
      apply_change deserializes and adds each (bank_id, account) tuple to the
      per-client set. The set deduplicates — repeated keys are absorbed,
      making replay idempotent.
  "eof"
      One upstream Q4Aggregator shard sent its EOF. Carries sender_id;
      apply_change advances the UpstreamEofCounter. Idempotent: duplicates
      are ignored.
  "close"
      The shard's dedup set + EOF counter are dropped and the client is marked
      closed. Late DATA messages for that client are ignored on WAL replay.
  "leader_report"  (shard 0 only, when deduper_amount > 1)
      One deduper shard's emitted-account count arrived. Records sender_id and
      accumulates the total. Idempotent: duplicate sender_ids are ignored by
      set membership. When all deduper_amount shards have reported, the live
      worker sends the gateway EOF (output lives in the durable outbox).
  "leader_close"   (shard 0 only, when deduper_amount > 1)
      All shards have reported; leader drops its accumulator for the client.
      Emitted immediately after the last "leader_report" in the same message.

When deduper_amount == 1 there are no leader_report / leader_close changes.

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  DATA message
    → data_change(client_id, payload)

  EOF from one upstream Q4Aggregator shard:
    → eof_change(client_id, sender_id)
    If eof_count(client_id) == aggregator_amount after applying:
      Read accounts_for(client_id) → emit deduplicated account batch to outbox.
      → close_change(client_id)
      If deduper_amount > 1 (not shard 0): send shard-report to shard 0.

  Shard report received (shard 0 only, deduper_amount > 1):
    shard_report = (sender_id, emitted_count) from a non-zero deduper shard.
    → leader_report_change(client_id, sender_id, emitted_accounts)
    If all deduper_amount shards have now reported:
      emit gateway EOF with leader_total(client_id).
      → leader_close_change(client_id)
      Bundle leader_report_change + leader_close_change for the last shard into
      a single compound change (one change per message).

State accessors (read before close_change / leader_close_change)
----------------------------------------------------------------
  accounts_for(client_id)    → set[tuple[str,str]]  deduplicated (bank_id, account) keys
  processed_count(client_id) → int
  eof_count(client_id)       → int  how many Q4Aggregator shards have sent EOF
  leader_total(client_id)    → int  accumulated emitted_accounts from all shards
  is_closed(client_id)       → bool
  is_leader_closed(client_id)→ bool
"""

from __future__ import annotations

import base64

from common.message_protocol.internal import Q4AccountIdSerializer
from common.upstream_eof_counter import UpstreamEofCounter


_account_ser = Q4AccountIdSerializer()


class Q4DeduperState:
    def __init__(self, aggregator_amount: int, deduper_amount: int) -> None:
        self._eof_counter = UpstreamEofCounter(aggregator_amount)
        # deduper_amount is the threshold for leader_report collection.
        self._deduper_amount = deduper_amount
        # Account keys are (bank_id, account) tuples — picklable and hashable.
        self._accounts_by_client: dict[int, set[tuple[str, str]]] = {}
        self._processed_by_client: dict[int, int] = {}
        self._closed_by_client: set[int] = set()
        # Leader state (only used on shard 0 when deduper_amount > 1).
        self._leader_reports_by_client: dict[int, set[int]] = {}
        self._leader_totals_by_client: dict[int, int] = {}
        self._leader_closed_by_client: set[int] = set()

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
    def leader_report_change(client_id: int, sender_id: int, emitted_accounts: int) -> dict:
        return {
            "type": "leader_report",
            "client_id": client_id,
            "sender_id": sender_id,
            "emitted_accounts": emitted_accounts,
        }

    @staticmethod
    def leader_close_change(client_id: int) -> dict:
        return {"type": "leader_close", "client_id": client_id}

    # ---------- state accessors (read before close_change) ----------

    def accounts_for(self, client_id: int) -> set[tuple[str, str]]:
        return self._accounts_by_client.get(client_id, set())

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def eof_count(self, client_id: int) -> int:
        return self._eof_counter.count(client_id)

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    def leader_total(self, client_id: int) -> int:
        return self._leader_totals_by_client.get(client_id, 0)

    def is_leader_closed(self, client_id: int) -> bool:
        return client_id in self._leader_closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        return {
            "accounts_by_client": {
                cid: set(keys)
                for cid, keys in self._accounts_by_client.items()
            },
            "processed_by_client": dict(self._processed_by_client),
            "eof_counter": self._eof_counter.snapshot(),
            "closed_by_client": set(self._closed_by_client),
            "leader_reports_by_client": {
                cid: set(ids)
                for cid, ids in self._leader_reports_by_client.items()
            },
            "leader_totals_by_client": dict(self._leader_totals_by_client),
            "leader_closed_by_client": set(self._leader_closed_by_client),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._accounts_by_client = {}
            self._processed_by_client = {}
            self._closed_by_client = set()
            self._leader_reports_by_client = {}
            self._leader_totals_by_client = {}
            self._leader_closed_by_client = set()
            return
        self._accounts_by_client = {
            cid: set(keys) for cid, keys in data["accounts_by_client"].items()
        }
        self._processed_by_client = dict(data["processed_by_client"])
        self._eof_counter.restore(data["eof_counter"])
        self._closed_by_client = set(data["closed_by_client"])
        self._leader_reports_by_client = {
            cid: set(ids) for cid, ids in data["leader_reports_by_client"].items()
        }
        self._leader_totals_by_client = dict(data["leader_totals_by_client"])
        self._leader_closed_by_client = set(data["leader_closed_by_client"])

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
        elif kind == "leader_report":
            self._apply_leader_report(client_id, change)
        elif kind == "leader_close":
            self._apply_leader_close(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    # ---------- private ----------

    def _apply_data(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        payload = base64.b64decode(change["payload_b64"])
        accounts = _account_ser.deserialize_batch(payload)
        keys = self._accounts_by_client.setdefault(client_id, set())
        for account in accounts:
            keys.add((account.bank_id, account.account))
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + len(accounts)
        )

    def _apply_eof(self, client_id: int, change: dict) -> None:
        # UpstreamEofCounter ignores duplicates — idempotent on WAL replay.
        self._eof_counter.on_eof(client_id, change["sender_id"])

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: popping a missing key is a no-op.
        self._accounts_by_client.pop(client_id, None)
        self._processed_by_client.pop(client_id, None)
        self._eof_counter.close(client_id)
        self._closed_by_client.add(client_id)

    def _apply_leader_report(self, client_id: int, change: dict) -> None:
        # Idempotent: duplicate sender_ids are ignored.
        if client_id in self._leader_closed_by_client:
            return
        sender_id = change["sender_id"]
        reports = self._leader_reports_by_client.setdefault(client_id, set())
        if sender_id in reports:
            return
        reports.add(sender_id)
        self._leader_totals_by_client[client_id] = (
            self._leader_totals_by_client.get(client_id, 0) + change["emitted_accounts"]
        )

    def _apply_leader_close(self, client_id: int) -> None:
        # Idempotent: called once all shards have reported.
        self._leader_reports_by_client.pop(client_id, None)
        self._leader_totals_by_client.pop(client_id, None)
        self._leader_closed_by_client.add(client_id)
