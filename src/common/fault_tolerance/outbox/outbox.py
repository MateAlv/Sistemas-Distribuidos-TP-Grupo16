"""Per-client pending downstream messages, kept until their input is committed.

Keyed as outbox[client_id][input_id] -> [OutboxEntry], so a whole input's
outputs are added and removed together. Entries are stored ready to publish.

Holds no files: persistence is via to_dict()/from_dict() (plain picklable data).
"""

from __future__ import annotations

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry


class Outbox:
    def __init__(self) -> None:
        self._pending: dict[int, dict[str, list[OutboxEntry]]] = {}

    def add(self, client_id: int, input_id: str, entries: list[OutboxEntry]) -> None:
        self._pending.setdefault(client_id, {})[input_id] = list(entries)

    def entries_for_input(self, client_id: int, input_id: str) -> list[OutboxEntry]:
        return self._pending.get(client_id, {}).get(input_id, [])

    def remove_input(self, client_id: int, input_id: str) -> None:
        inputs = self._pending.get(client_id)
        if inputs is None:
            return
        inputs.pop(input_id, None)
        if not inputs:
            del self._pending[client_id]

    def all_pending(self) -> list[OutboxEntry]:
        return [
            entry
            for inputs in self._pending.values()
            for entries in inputs.values()
            for entry in entries
        ]

    def drop_client(self, client_id: int) -> None:
        self._pending.pop(client_id, None)

    def to_dict(self) -> dict:
        return {
            client_id: {
                input_id: [_entry_to_dict(entry) for entry in entries]
                for input_id, entries in inputs.items()
            }
            for client_id, inputs in self._pending.items()
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Outbox":
        outbox = cls()
        outbox._pending = {
            client_id: {
                input_id: [_entry_from_dict(entry) for entry in entries]
                for input_id, entries in inputs.items()
            }
            for client_id, inputs in data.items()
        }
        return outbox


def _entry_to_dict(entry: OutboxEntry) -> dict:
    return {
        "output_id": entry.output_id,
        "input_id": entry.input_id,
        "destination": entry.destination,
        "body": entry.body,
    }


def _entry_from_dict(data: dict) -> OutboxEntry:
    return OutboxEntry(
        output_id=data["output_id"],
        input_id=data["input_id"],
        destination=data["destination"],
        body=data["body"],
    )
