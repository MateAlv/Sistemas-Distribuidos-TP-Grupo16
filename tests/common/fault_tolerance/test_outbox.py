from common.fault_tolerance.outbox.outbox import Outbox
from common.fault_tolerance.outbox.outbox_entry import OutboxEntry

CLIENT = 123


def _entry(output_id: str, input_id: str, body: bytes = b"x") -> OutboxEntry:
    return OutboxEntry(output_id, input_id, "dest", body)


def test_add_and_read_back():
    outbox = Outbox()
    entries = [_entry("o#0", "in1"), _entry("o#1", "in1")]
    outbox.add(CLIENT, "in1", entries)
    assert outbox.entries_for_input(CLIENT, "in1") == entries


def test_missing_input_returns_empty():
    outbox = Outbox()
    assert outbox.entries_for_input(CLIENT, "nope") == []


def test_remove_input():
    outbox = Outbox()
    outbox.add(CLIENT, "in1", [_entry("o#0", "in1")])
    outbox.remove_input(CLIENT, "in1")
    assert outbox.entries_for_input(CLIENT, "in1") == []


def test_remove_last_input_prunes_client():
    outbox = Outbox()
    outbox.add(CLIENT, "in1", [_entry("o#0", "in1")])
    outbox.remove_input(CLIENT, "in1")
    assert outbox.all_pending() == []
    # adding again still works after the client entry was pruned
    outbox.add(CLIENT, "in2", [_entry("o#0", "in2")])
    assert len(outbox.all_pending()) == 1


def test_remove_unknown_input_is_noop():
    outbox = Outbox()
    outbox.add(CLIENT, "in1", [_entry("o#0", "in1")])
    outbox.remove_input(CLIENT, "other")
    outbox.remove_input(999, "in1")
    assert len(outbox.all_pending()) == 1


def test_all_pending_spans_clients_and_inputs():
    outbox = Outbox()
    outbox.add(1, "a", [_entry("1a#0", "a")])
    outbox.add(1, "b", [_entry("1b#0", "b"), _entry("1b#1", "b")])
    outbox.add(2, "a", [_entry("2a#0", "a")])
    assert len(outbox.all_pending()) == 4


def test_drop_client():
    outbox = Outbox()
    outbox.add(1, "a", [_entry("1a#0", "a")])
    outbox.add(2, "a", [_entry("2a#0", "a")])
    outbox.drop_client(1)
    assert outbox.entries_for_input(1, "a") == []
    assert len(outbox.all_pending()) == 1


def test_add_overwrites_same_input():
    outbox = Outbox()
    outbox.add(CLIENT, "in1", [_entry("o#0", "in1")])
    outbox.add(CLIENT, "in1", [_entry("o#0", "in1"), _entry("o#1", "in1")])
    assert len(outbox.entries_for_input(CLIENT, "in1")) == 2


def test_round_trip_empty():
    restored = Outbox.from_dict(Outbox().to_dict())
    assert restored.all_pending() == []


def test_round_trip_preserves_structure_and_bytes():
    outbox = Outbox()
    outbox.add(1, "a", [_entry("1a#0", "a", body=b"\x00\x01payload")])
    outbox.add(1, "b", [_entry("1b#0", "b"), _entry("1b#1", "b", body=b"")])
    outbox.add(2, "a", [_entry("2a#0", "a", body=b"\xff" * 300)])

    restored = Outbox.from_dict(outbox.to_dict())

    assert restored.entries_for_input(1, "a") == outbox.entries_for_input(1, "a")
    assert restored.entries_for_input(1, "b") == outbox.entries_for_input(1, "b")
    assert restored.entries_for_input(2, "a") == outbox.entries_for_input(2, "a")
    assert len(restored.all_pending()) == 4
