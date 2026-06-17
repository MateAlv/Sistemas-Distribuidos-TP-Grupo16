"""Outbox: per-client pending downstream messages.

Nested keying (decision D3): outbox[client_id][input_id] -> [OutboxEntry].
Entries store ready-to-publish bytes (decision D4). Whole input removed on
INPUT_DONE; whole client dropped on client-finished.

Does NO file IO: to_dict()/from_dict() only.

"""

from __future__ import annotations

from common.fault_tolerance.records import OutboxEntry


class Outbox:
    def __init__(self) -> None:
        # client_id -> input_id -> [OutboxEntry]
        self._pending: dict[int, dict[str, list[OutboxEntry]]] = {}

    def add(self, client_id: int, input_id: str, entries: list[OutboxEntry]) -> None:
        raise NotImplementedError

    def entries_for_input(self, client_id: int, input_id: str) -> list[OutboxEntry]:
        raise NotImplementedError

    def remove_input(self, client_id: int, input_id: str) -> None:
        raise NotImplementedError

    def all_pending(self) -> list[OutboxEntry]:
        """Every unconfirmed entry, for republish on recovery (PDF steps 10-11)."""
        raise NotImplementedError

    def drop_client(self, client_id: int) -> None:
        raise NotImplementedError

    def to_dict(self) -> dict:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: dict) -> "Outbox":
        raise NotImplementedError
