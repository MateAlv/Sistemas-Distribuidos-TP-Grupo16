from common.fault_tolerance.handler.sender_sequencer import EdgeSpec, SenderSequencer
from common.message_protocol.internal.common.message_type import MessageType
from common.message_protocol.internal.protocol import InternalProtocol
from common.middleware.sharded_publisher import addressed_body_digest_key
from common.routing import shard_for_key

CLIENT = 7


def _seq(node="node_a"):
    return SenderSequencer(node)


def _plain(payload: bytes, client: int = CLIENT, msg_type=MessageType.DATA) -> bytes:
    return InternalProtocol.create_packet(
        msg_type, client.to_bytes(16, "big"), payload
    )


def test_stamp_builds_deterministic_ids_without_advancing():
    sequencer = _seq()
    first = sequencer.stamp(CLIENT, "in", [("dest", b"a")])
    second = sequencer.stamp(CLIENT, "in", [("dest", b"a")])
    # No advance() in between, so the same seq is reused (stamp is pure).
    assert first[0].output_id == "node_a:7:0#0"
    assert second[0].output_id == "node_a:7:0#0"


def test_advance_moves_the_counter_per_client():
    sequencer = _seq()
    sequencer.advance(CLIENT, 2)
    entries = sequencer.stamp(CLIENT, "in", [("dest", b"a"), ("dest", b"b")])
    assert [e.output_id for e in entries] == ["node_a:7:2#0", "node_a:7:3#1"]


def test_index_is_within_the_input_batch():
    sequencer = _seq()
    entries = sequencer.stamp(CLIENT, "in", [("d", b"a"), ("d", b"b"), ("d", b"c")])
    assert [e.output_id for e in entries] == [
        "node_a:7:0#0",
        "node_a:7:1#1",
        "node_a:7:2#2",
    ]


def test_counters_are_independent_per_client():
    sequencer = _seq()
    sequencer.advance(1, 5)
    assert sequencer.stamp(1, "in", [("d", b"x")])[0].output_id == "node_a:1:5#0"
    assert sequencer.stamp(2, "in", [("d", b"x")])[0].output_id == "node_a:2:0#0"


def test_observe_restores_high_water_from_replayed_outputs():
    sequencer = _seq()
    replayed = sequencer.stamp(CLIENT, "in", [("d", b"a"), ("d", b"b")])  # seq 0,1
    fresh = _seq()
    fresh.observe(replayed)
    assert fresh.stamp(CLIENT, "in", [("d", b"c")])[0].output_id == "node_a:7:2#0"


def test_observe_ignores_lower_seqs():
    sequencer = _seq()
    sequencer.advance(CLIENT, 10)
    sequencer.observe([sequencer.stamp(CLIENT, "in", [("d", b"x")])[0]])  # seq 10
    # an out-of-order lower id must not pull the counter back
    sequencer.observe([SenderSequencer("node_a").stamp(CLIENT, "in", [("d", b"y")])[0]])
    assert sequencer.stamp(CLIENT, "in", [("d", b"z")])[0].output_id == "node_a:7:11#0"


def test_round_trip_through_dict_preserves_counters():
    sequencer = _seq()
    sequencer.advance(CLIENT, 3)
    sequencer.advance(99, 1)
    restored = SenderSequencer.from_dict("node_a", sequencer.to_dict())
    assert restored.stamp(CLIENT, "in", [("d", b"x")])[0].output_id == "node_a:7:3#0"
    assert restored.stamp(99, "in", [("d", b"x")])[0].output_id == "node_a:99:1#0"


# ---------- addressed edges (per (client, edge, shard) dense seq) ----------

EDGE = "q4_sum"
SHARDS = 4


def _addr_seq(node="node_a", sender_id=2, shard_count=SHARDS):
    return SenderSequencer(node, {EDGE: EdgeSpec(sender_id=sender_id, shard_count=shard_count)})


def _payload_for_shard(target: int, shard_count: int = SHARDS) -> bytes:
    for n in range(100000):
        p = f"p{n}".encode()
        if shard_for_key(p, shard_count) == target:
            return p
    raise AssertionError("no payload found for shard")


def test_addressed_edge_emits_addressed_body_with_sender_and_seq():
    sequencer = _addr_seq(sender_id=5)
    payload = b"hello"
    entry = sequencer.stamp(CLIENT, "in", [(EDGE, _plain(payload))])[0]
    msg_type, client_id, sender_id, seq, body = InternalProtocol.unpack_addressed_packet(
        entry.body
    )
    assert msg_type == MessageType.DATA
    assert client_id == CLIENT
    assert sender_id == 5
    assert seq == 0
    assert body == payload


def test_addressed_seq_is_dense_per_shard():
    sequencer = _addr_seq()
    p0 = _payload_for_shard(0)
    p1 = _payload_for_shard(1)
    # Two messages to shard 0 and one to shard 1, interleaved across inputs.
    a = sequencer.stamp(CLIENT, "i1", [(EDGE, _plain(p0))])[0]
    sequencer.observe([a])
    b = sequencer.stamp(CLIENT, "i2", [(EDGE, _plain(p1))])[0]
    sequencer.observe([b])
    c = sequencer.stamp(CLIENT, "i3", [(EDGE, _plain(p0))])[0]

    def seq_of(entry):
        return InternalProtocol.unpack_addressed_packet(entry.body)[3]

    assert seq_of(a) == 0  # shard 0, first
    assert seq_of(b) == 0  # shard 1, first (independent counter)
    assert seq_of(c) == 1  # shard 0, second (dense, no gap from shard 1)


def test_addressed_output_id_encodes_edge_and_shard():
    sequencer = _addr_seq()
    p = _payload_for_shard(3)
    entry = sequencer.stamp(CLIENT, "in", [(EDGE, _plain(p))])[0]
    assert entry.output_id == f"node_a:{CLIENT}:{EDGE}:3:0#0"


def test_sequencer_shard_matches_publisher_routing_key():
    # The seq is reserved for shard_for_key(payload). The publisher must route the
    # addressed body to that same shard, or two messages could collide on (shard, seq).
    sequencer = _addr_seq()
    p = _payload_for_shard(2)
    entry = sequencer.stamp(CLIENT, "in", [(EDGE, _plain(p))])[0]
    publisher_shard = shard_for_key(addressed_body_digest_key(entry.body), SHARDS)
    assert publisher_shard == 2


def test_two_outputs_same_input_same_shard_get_consecutive_seqs():
    sequencer = _addr_seq()
    p = _payload_for_shard(1)
    entries = sequencer.stamp(CLIENT, "in", [(EDGE, _plain(p)), (EDGE, _plain(p))])

    def seq_of(entry):
        return InternalProtocol.unpack_addressed_packet(entry.body)[3]

    assert [seq_of(e) for e in entries] == [0, 1]
    assert [e.output_id for e in entries] == [
        f"node_a:{CLIENT}:{EDGE}:1:0#0",
        f"node_a:{CLIENT}:{EDGE}:1:1#1",
    ]


def test_observe_restores_addressed_high_water_per_shard():
    sequencer = _addr_seq()
    p0 = _payload_for_shard(0)
    replayed = sequencer.stamp(CLIENT, "in", [(EDGE, _plain(p0)), (EDGE, _plain(p0))])
    fresh = _addr_seq()
    fresh.observe(replayed)
    nxt = fresh.stamp(CLIENT, "in2", [(EDGE, _plain(p0))])[0]
    assert InternalProtocol.unpack_addressed_packet(nxt.body)[3] == 2


def test_round_trip_preserves_addressed_counters():
    sequencer = _addr_seq()
    p0 = _payload_for_shard(0)
    p3 = _payload_for_shard(3)
    sequencer.observe(sequencer.stamp(CLIENT, "in", [(EDGE, _plain(p0))]))
    sequencer.observe(sequencer.stamp(CLIENT, "in", [(EDGE, _plain(p3))]))
    restored = SenderSequencer.from_dict(
        "node_a", sequencer.to_dict(), {EDGE: EdgeSpec(sender_id=2, shard_count=SHARDS)}
    )
    assert InternalProtocol.unpack_addressed_packet(
        restored.stamp(CLIENT, "in2", [(EDGE, _plain(p0))])[0].body
    )[3] == 1


def test_explicit_shard_overrides_digest_and_keys_seq():
    sequencer = _addr_seq()
    # Same payload (would hash to one digest shard) sent to two explicit shards.
    p = b"same-payload"
    e2 = sequencer.stamp(CLIENT, "i1", [(EDGE, _plain(p), 2)])[0]
    sequencer.observe([e2])
    e2b = sequencer.stamp(CLIENT, "i2", [(EDGE, _plain(p), 2)])[0]
    e5 = sequencer.stamp(CLIENT, "i3", [(EDGE, _plain(p), 5)])[0]

    def parse(entry):
        _, _, _, seq, _ = InternalProtocol.unpack_addressed_packet(entry.body)
        return entry.shard, seq

    assert parse(e2) == (2, 0)
    assert parse(e2b) == (2, 1)   # dense per explicit shard 2
    assert parse(e5) == (5, 0)    # independent counter for explicit shard 5
    assert e2.output_id == f"node_a:{CLIENT}:{EDGE}:2:0#0"


def test_explicit_shard_on_plain_edge_routes_without_addressing():
    sequencer = _addr_seq()
    entry = sequencer.stamp(CLIENT, "in", [("plain_edge", _plain(b"x"), 3)])[0]
    assert entry.shard == 3                 # carried for routing
    assert entry.body == _plain(b"x")       # body untouched (edge not configured)
    assert entry.output_id == f"node_a:{CLIENT}:0#0"


def test_unconfigured_dest_stays_plain_when_edges_present():
    sequencer = _addr_seq()
    entry = sequencer.stamp(CLIENT, "in", [("control_q", _plain(b"x"))])[0]
    assert entry.output_id == f"node_a:{CLIENT}:0#0"
    assert entry.body == _plain(b"x")  # untouched, still plain


def test_node_id_with_colon_is_rejected():
    import pytest

    with pytest.raises(ValueError):
        SenderSequencer("bad:node")
