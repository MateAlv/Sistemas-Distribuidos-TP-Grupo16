from common.batch_buffer import BatchBuffer


def test_append_returns_none_before_limit():
    buffer = BatchBuffer(max_bytes=1024, max_items=10)

    assert buffer.append("k", b"a") is None
    assert buffer.append("k", b"b") is None


def test_append_flushes_when_max_items_reached():
    buffer = BatchBuffer(max_bytes=1024, max_items=3)

    assert buffer.append("k", b"a") is None
    assert buffer.append("k", b"b") is None
    assert buffer.append("k", b"c") == b"abc"
    # After the automatic flush the buffer is empty.
    assert buffer.flush(lambda _: True) == []


def test_append_flushes_when_max_bytes_reached():
    buffer = BatchBuffer(max_bytes=4, max_items=100)

    assert buffer.append("k", b"aa") is None
    assert buffer.append("k", b"bb") == b"aabb"


def test_flush_selects_by_predicate():
    buffer = BatchBuffer(max_bytes=1024, max_items=100)

    buffer.append(("client", 1, "a"), b"x")
    buffer.append(("client", 1, "a"), b"y")
    buffer.append(("client", 2, "a"), b"z")

    flushed = buffer.flush(lambda k: k[1] == 1)

    assert flushed == [(("client", 1, "a"), b"xy")]
    # Flushing empties only the matching keys.
    assert buffer.flush(lambda k: k[1] == 1) == []
    assert buffer.flush(lambda k: k[1] == 2) == [(("client", 2, "a"), b"z")]


def test_discard_removes_without_returning():
    buffer = BatchBuffer(max_bytes=1024, max_items=100)

    buffer.append("k", b"x")
    buffer.discard(lambda _: True)

    assert buffer.flush(lambda _: True) == []
