"""WorkerState adapter for the q2_bank_name_joiner worker.

Wraps the joiner's per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

This worker receives data from two independent streams and emits once both
are fully received (ClientState.ready()). No EofCoordinator is used.

Change types
------------
  "q2_data"
      One Q2 partial result arrived. Carries the raw payload (base64) so
      apply_change can replay the exact deserialization and notebook_bank_id
      normalisation that the live worker applies.
  "q2_eof"
      The Q2 stream EOF arrived. Sets q2_expected_total in ClientState so
      ready() can detect completion.
  "accounts_data"
      One accounts batch arrived. Carries the already-parsed (bank_id, bank_name)
      mappings rather than the raw LineBatch payload — the adapter does not need
      to depend on LineBatchSerializer or parse_csv_line.
  "accounts_eof"
      The accounts stream EOF arrived. Sets accounts_expected_total.
  "close"
      The client was fully emitted and cleaned up. Drops ClientState and marks
      the client closed.

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  Q2 DATA message
    → q2_data_change(client_id, payload)

  Q2 EOF message
    → q2_eof_change(client_id, expected_total)
    If state_for(client_id).ready() after applying:
      Emit joined results to outbox.
      → close_change(client_id)

  Accounts DATA message
    mappings = parse_accounts_batch(payload)   # list[(bank_id, bank_name)]
    → accounts_data_change(client_id, mappings)
    If state_for(client_id).ready() after applying:
      Emit joined results to outbox.
      → close_change(client_id)

  Accounts EOF message
    → accounts_eof_change(client_id, expected_total)
    If state_for(client_id).ready() after applying:
      Emit joined results to outbox.
      → close_change(client_id)

State accessors (read before close_change)
------------------------------------------
  state_for(client_id) → ClientState  full join state (q2_results, bank_names, counters)
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field

from common.bank_ids import notebook_bank_id
from common.domain.partial_result import Q2BankMaxPartial
from common.message_protocol.internal import Q2BankMaxPartialSerializer


@dataclass
class ClientState:
    bank_names: dict[str, list[str]] = field(default_factory=dict)
    q2_results: dict[str, Q2BankMaxPartial] = field(default_factory=dict)
    q2_data_count: int = 0
    q2_expected_total: int | None = None
    accounts_batch_count: int = 0
    accounts_expected_total: int | None = None

    def ready(self) -> bool:
        if self.q2_expected_total is None or self.accounts_expected_total is None:
            return False
        if self.q2_data_count < self.q2_expected_total:
            return False
        if self.accounts_batch_count < self.accounts_expected_total:
            return False
        return True


class BankNameJoinerState:
    def __init__(self) -> None:
        self._states: dict[int, ClientState] = {}
        # Set is sufficient; the live worker stores timestamps for LRU eviction
        # but the adapter only needs closed/not-closed.
        self._closed_by_client: set[int] = set()

    @staticmethod
    def q2_data_change(client_id: int, payload: bytes) -> dict:
        # payload is base64 because the WAL frames state_change dicts as JSON.
        return {
            "type": "q2_data",
            "client_id": client_id,
            "payload_b64": base64.b64encode(payload).decode("ascii"),
        }

    @staticmethod
    def q2_eof_change(client_id: int, expected_total: int) -> dict:
        return {"type": "q2_eof", "client_id": client_id, "expected_total": expected_total}

    @staticmethod
    def accounts_data_change(
        client_id: int, mappings: list[tuple[str, str]]
    ) -> dict:
        # Carry parsed mappings rather than the raw LineBatch payload so that
        # apply_change does not need to re-run LineBatchSerializer + parse_csv_line.
        return {
            "type": "accounts_data",
            "client_id": client_id,
            "mappings": [[bank_id, bank_name] for bank_id, bank_name in mappings],
        }

    @staticmethod
    def accounts_eof_change(client_id: int, expected_total: int) -> dict:
        return {
            "type": "accounts_eof",
            "client_id": client_id,
            "expected_total": expected_total,
        }

    @staticmethod
    def close_change(client_id: int) -> dict:
        return {"type": "close", "client_id": client_id}

    def client_state(self, client_id: int) -> ClientState | None:
        """Read-only view of accumulated per-client state; None if not yet seen."""
        return self._states.get(client_id)

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        # ClientState and Q2BankMaxPartial are plain dataclasses with primitive
        # fields — stored directly since pickle handles them without extra work.
        return {
            "states": dict(self._states),
            "closed_by_client": set(self._closed_by_client),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._states = {}
            self._closed_by_client = set()
            return
        self._states = dict(data["states"])
        self._closed_by_client = set(data["closed_by_client"])

    def apply_change(self, change: dict) -> None:
        # Single mutation path — runs both live and during WAL replay.
        kind = change["type"]
        client_id = change["client_id"]
        if kind == "q2_data":
            self._apply_q2_data(client_id, change)
        elif kind == "q2_eof":
            self._apply_q2_eof(client_id, change)
        elif kind == "accounts_data":
            self._apply_accounts_data(client_id, change)
        elif kind == "accounts_eof":
            self._apply_accounts_eof(client_id, change)
        elif kind == "close":
            self._apply_close(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    def _apply_q2_data(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        payload = base64.b64decode(change["payload_b64"])
        partial = Q2BankMaxPartialSerializer.deserialize(payload)
        # notebook_bank_id normalises bank IDs to the canonical form used in the
        # notebook; must be applied here to match the live worker's key.
        bank_id = notebook_bank_id(partial.bank_id)
        if bank_id != partial.bank_id:
            partial = Q2BankMaxPartial(
                bank_id=bank_id,
                from_account=partial.from_account,
                amount=partial.amount,
            )
        state = self._state_for(client_id)
        state.q2_results[bank_id] = partial
        state.q2_data_count += 1

    def _apply_q2_eof(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        self._state_for(client_id).q2_expected_total = change["expected_total"]

    def _apply_accounts_data(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        state = self._state_for(client_id)
        for bank_id, bank_name in change["mappings"]:
            bank_names = state.bank_names.setdefault(bank_id, [])
            if bank_name not in bank_names:
                bank_names.append(bank_name)
        state.accounts_batch_count += 1

    def _apply_accounts_eof(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        self._state_for(client_id).accounts_expected_total = change["expected_total"]

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: popping a missing client is a no-op.
        self._states.pop(client_id, None)
        self._closed_by_client.add(client_id)

    def _state_for(self, client_id: int) -> ClientState:
        state = self._states.get(client_id)
        if state is None:
            state = ClientState()
            self._states[client_id] = state
        return state
