"""Assigns durable, deterministic ids to outgoing messages.

Business code returns plain ``(destination, body)`` outputs; the handler routes
them through here so id assignment lives in exactly one place. Each output gets

    output_id = "{node_id}:{client_id}:{seq}#{index}"

where ``seq`` is a per-client counter (the integer a downstream worker dedups on)
and ``index`` is the output's position within its originating input. The counter
is persisted in the snapshot, so an id is never reused for different content and
never resets to 0 after a restart.

Determinism on recovery does not come from recomputing ids: the stamped outputs
are persisted in the WAL/outbox and resent verbatim. The sequencer only needs to
restore its high-water mark, which ``observe`` rebuilds from replayed outputs.
"""

from __future__ import annotations

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry

LogicalOutput = tuple[str, bytes]


class SenderSequencer:
    def __init__(self, node_id: str) -> None:
        self._node_id = node_id
        self._next_seq: dict[int, int] = {}

    def stamp(
        self, client_id: int, input_id: str, outputs: list[LogicalOutput]
    ) -> list[OutboxEntry]:
        """Turn logical outputs into addressed OutboxEntries using the current
        counter, without advancing it (the counter is a memory mutation, so it
        only commits after the WAL append, via advance)."""
        base = self._next_seq.get(client_id, 0)
        return [
            OutboxEntry(
                output_id=f"{self._node_id}:{client_id}:{base + index}#{index}",
                input_id=input_id,
                destination=destination,
                body=body,
            )
            for index, (destination, body) in enumerate(outputs)
        ]

    def advance(self, client_id: int, count: int) -> None:
        self._next_seq[client_id] = self._next_seq.get(client_id, 0) + count

    def observe(self, outputs: list[OutboxEntry]) -> None:
        """Restore the high-water mark from already-stamped outputs seen during
        replay, so freshly stamped ids never collide with persisted ones."""
        for entry in outputs:
            client_id, seq = self._parse(entry.output_id)
            if seq + 1 > self._next_seq.get(client_id, 0):
                self._next_seq[client_id] = seq + 1

    def to_dict(self) -> dict:
        return {"next_seq": dict(self._next_seq)}

    @classmethod
    def from_dict(cls, node_id: str, data: dict) -> "SenderSequencer":
        sequencer = cls(node_id)
        sequencer._next_seq = {
            int(client_id): int(seq)
            for client_id, seq in data.get("next_seq", {}).items()
        }
        return sequencer

    @staticmethod
    def _parse(output_id: str) -> tuple[int, int]:
        head, _index = output_id.rsplit("#", 1)
        _node, client_id, seq = head.rsplit(":", 2)
        return int(client_id), int(seq)
