from common.fault_tolerance.handler.sender_sequencer import SenderSequencer

CLIENT = 7


def _seq(node="node_a"):
    return SenderSequencer(node)


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
