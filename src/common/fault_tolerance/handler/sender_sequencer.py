"""Assigns durable, deterministic ids to outgoing messages.

Business code returns plain ``(destination, body)`` outputs; the handler routes
them through here so id assignment lives in exactly one place.

Two stamping modes coexist, chosen per destination:

* **Plain (legacy)** — when the destination has no edge spec. The body is left
  untouched and the id is

      output_id = "{node_id}:{client_id}:{seq}#{index}"

  where ``seq`` is a per-client counter and ``index`` the output's position in
  its originating input. Used for control/response queues and for any data edge
  whose consumer still reads plain packets.

* **Addressed** — when the destination has an :class:`EdgeSpec`. The body is
  rebuilt as an *addressed* packet carrying ``sender_id`` and ``seq`` so the
  downstream worker can dedup on ``(client, kind, sender_id, seq)``. The ``seq``
  is kept dense per ``(client_id, edge, shard)``: the edge shards by payload
  digest, so a per-client counter would leave each shard seeing ~1/N of the seqs
  (unbounded gaps in the consumer's dedup tracker). The id is

      output_id = "{node_id}:{client_id}:{edge}:{shard}:{seq}#{index}"

The shard is whatever the worker passes in the logical output (semantic
partitioning, the Q4 case); if it passes none, it falls back to
``shard_for_key(payload, shard_count)`` (digest sharding). Either way the chosen
shard is recorded on the OutboxEntry so the publisher routes to the exact shard
the seq was assigned for.

Determinism on recovery does not come from recomputing ids: the stamped outputs
are persisted in the WAL/outbox and resent verbatim. The sequencer only needs to
restore its high-water marks, which ``observe`` rebuilds from replayed outputs.
"""

from __future__ import annotations

from dataclasses import dataclass

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.message_protocol.internal.protocol import InternalProtocol
from common.routing import shard_for_key

# A logical output is (destination, body) or (destination, body, shard); shard is
# the explicit destination partition (None → digest sharding for addressed edges,
# or let the publisher route for plain edges).
LogicalOutput = tuple[str, bytes] | tuple[str, bytes, "int | None"]


def _normalize(output: LogicalOutput) -> tuple[str, bytes, "int | None"]:
    if len(output) == 3:
        destination, body, shard = output
        return destination, body, shard
    destination, body = output
    return destination, body, None


@dataclass(frozen=True)
class EdgeSpec:
    """Addressing config for one logical output edge.

    sender_id    this producer's stable instance id, stamped into every packet.
    shard_count  destination shard count; the seq is kept dense per
                 (client, edge, shard) so the consumer's dedup tracker stays
                 bounded.
    """

    sender_id: int
    shard_count: int


class SenderSequencer:
    def __init__(self, node_id: str, edges: dict[str, EdgeSpec] | None = None) -> None:
        if ":" in node_id:
            raise ValueError("node_id must not contain ':'")
        self._node_id = node_id
        self._edges = dict(edges or {})
        self._next_seq: dict[int, int] = {}
        self._next_addr_seq: dict[tuple[int, str, int], int] = {}

    def stamp(
        self, client_id: int, input_id: str, outputs: list[LogicalOutput]
    ) -> list[OutboxEntry]:
        """Turn logical outputs into OutboxEntries using the current counters,
        without advancing them (the counter is a memory mutation, so it only
        commits after the WAL append, via observe)."""
        local_plain: dict[int, int] = {}
        local_addr: dict[tuple[int, str, int], int] = {}
        entries: list[OutboxEntry] = []
        for index, output in enumerate(outputs):
            destination, body, out_shard = _normalize(output)
            spec = self._edges.get(destination)
            if spec is None:
                base = self._next_seq.get(client_id, 0)
                offset = local_plain.get(client_id, 0)
                local_plain[client_id] = offset + 1
                seq = base + offset
                entries.append(
                    OutboxEntry(
                        output_id=f"{self._node_id}:{client_id}:{seq}#{index}",
                        input_id=input_id,
                        destination=destination,
                        body=body,
                        shard=out_shard,
                    )
                )
                continue

            msg_type, _client, payload = InternalProtocol.unpack_packet(body)
            shard = (
                out_shard
                if out_shard is not None
                else shard_for_key(payload, spec.shard_count)
            )
            bucket = (client_id, destination, shard)
            base = self._next_addr_seq.get(bucket, 0)
            offset = local_addr.get(bucket, 0)
            local_addr[bucket] = offset + 1
            seq = base + offset
            addressed = InternalProtocol.create_addressed_packet(
                msg_type,
                client_id.to_bytes(16, byteorder="big"),
                spec.sender_id,
                seq,
                payload,
            )
            entries.append(
                OutboxEntry(
                    output_id=f"{self._node_id}:{client_id}:{destination}:{shard}:{seq}#{index}",
                    input_id=input_id,
                    destination=destination,
                    body=addressed,
                    shard=shard,
                )
            )
        return entries

    def advance(self, client_id: int, count: int) -> None:
        """Bump the legacy per-client counter. Kept for plain-only callers; the
        handler uses observe() so mixed plain/addressed inputs advance correctly."""
        self._next_seq[client_id] = self._next_seq.get(client_id, 0) + count

    def observe(self, outputs: list[OutboxEntry]) -> None:
        """Restore high-water marks from already-stamped outputs, so freshly
        stamped ids never collide with persisted ones. Drives both the live
        advance (after a WAL append) and the replay advance (during recovery)."""
        for entry in outputs:
            parsed = self._parse(entry.output_id)
            if parsed is None:
                continue
            if parsed[0] == "plain":
                _, client_id, seq = parsed
                if seq + 1 > self._next_seq.get(client_id, 0):
                    self._next_seq[client_id] = seq + 1
            else:
                _, client_id, edge, shard, seq = parsed
                bucket = (client_id, edge, shard)
                if seq + 1 > self._next_addr_seq.get(bucket, 0):
                    self._next_addr_seq[bucket] = seq + 1

    def to_dict(self) -> dict:
        return {
            "next_seq": dict(self._next_seq),
            "next_addr_seq": [
                [client_id, edge, shard, seq]
                for (client_id, edge, shard), seq in self._next_addr_seq.items()
            ],
        }

    @classmethod
    def from_dict(
        cls, node_id: str, data: dict, edges: dict[str, EdgeSpec] | None = None
    ) -> "SenderSequencer":
        sequencer = cls(node_id, edges)
        sequencer._next_seq = {
            int(client_id): int(seq)
            for client_id, seq in data.get("next_seq", {}).items()
        }
        sequencer._next_addr_seq = {
            (int(client_id), str(edge), int(shard)): int(seq)
            for client_id, edge, shard, seq in data.get("next_addr_seq", [])
        }
        return sequencer

    @staticmethod
    def _parse(output_id: str):
        head, _index = output_id.rsplit("#", 1)
        colons = head.count(":")
        if colons == 2:
            _node, client_id, seq = head.split(":")
            return ("plain", int(client_id), int(seq))
        if colons == 4:
            _node, client_id, edge, shard, seq = head.split(":")
            return ("addressed", int(client_id), edge, int(shard), int(seq))
        return None
