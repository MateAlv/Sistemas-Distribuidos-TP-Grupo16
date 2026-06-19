from common.fault_tolerance.inbox.watermark import Watermark


def test_fresh_watermark_treats_everything_as_new():
    wm = Watermark()
    assert wm.is_duplicate(1) is False


def test_in_order_sequence_has_no_gaps():
    wm = Watermark()
    for seq in (1, 2, 3):
        wm.observe(seq)
    assert wm.biggest == 3
    assert wm.pending == set()


def test_out_of_order_records_the_gaps():
    wm = Watermark()
    wm.observe(1)
    wm.observe(2)
    wm.observe(5)
    assert wm.biggest == 5
    assert wm.pending == {3, 4}


def test_duplicate_below_biggest_and_not_pending():
    wm = Watermark()
    wm.observe(1)
    wm.observe(2)
    wm.observe(5)
    assert wm.is_duplicate(2) is True


def test_gap_filling_is_new_then_duplicate():
    wm = Watermark()
    wm.observe(1)
    wm.observe(2)
    wm.observe(5)

    assert wm.is_duplicate(3) is False  # still a pending gap -> new
    wm.observe(3)
    assert wm.pending == {4}
    assert wm.is_duplicate(3) is True   # now filled -> duplicate


def test_value_above_biggest_is_new():
    wm = Watermark()
    wm.observe(5)
    assert wm.is_duplicate(6) is False


def test_observing_same_seq_twice_is_idempotent():
    wm = Watermark()
    wm.observe(3)        # biggest=3, pending={1,2}
    wm.observe(1)        # fills gap
    wm.observe(1)        # no effect
    assert wm.pending == {2}


def test_round_trip_preserves_state():
    wm = Watermark()
    wm.observe(1)
    wm.observe(5)
    restored, offset = Watermark.deserialize(wm.serialize())
    assert offset == len(wm.serialize())
    assert restored.biggest == 5
    assert restored.pending == {2, 3, 4}
    assert restored.is_duplicate(2) is False
    assert restored.is_duplicate(1) is True
