"""Per-client pending downstream messages, kept until their input is committed.

Keyed as outbox[client_id][input_id] -> [OutboxEntry], so a whole input's
outputs are added and removed together. Entries are stored ready to publish.

Holds no files: persistence is via to_dict()/from_dict().
"""

from __future__ import annotations

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry


class Outbox:
    def __init__(self) -> None:
        self._pending: dict[int, dict[str, list[OutboxEntry]]] = {}

    def add(self, client_id: int, input_id: str, entries: list[OutboxEntry]) -> None:
        raise NotImplementedError

    def entries_for_input(self, client_id: int, input_id: str) -> list[OutboxEntry]:
        raise NotImplementedError

    def remove_input(self, client_id: int, input_id: str) -> None:
        raise NotImplementedError

    def all_pending(self) -> list[OutboxEntry]:
        """Every unconfirmed entry, for republishing on recovery."""
        raise NotImplementedError

    def drop_client(self, client_id: int) -> None:
        raise NotImplementedError

    def to_dict(self) -> dict:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: dict) -> "Outbox":
        raise NotImplementedError
