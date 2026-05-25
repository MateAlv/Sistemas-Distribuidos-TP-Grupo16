from workers.filter.output_batcher import OutputBatcher


class _FakeSerializer:
    """Serializer determinista para los tests: cada tx se convierte en su
    representacion bytes (asi controlamos el tamano facilmente)."""

    def serialize(self, tx) -> bytes:
        return str(tx).encode("utf-8")


def test_append_returns_none_before_limit():
    batcher = OutputBatcher(_FakeSerializer(), max_bytes=1024, max_tx=10)

    assert batcher.append("q", 1, "a") is None
    assert batcher.append("q", 1, "b") is None


def test_append_flushes_when_max_tx_reached():
    batcher = OutputBatcher(_FakeSerializer(), max_bytes=1024, max_tx=3)

    assert batcher.append("q", 1, "a") is None
    assert batcher.append("q", 1, "b") is None
    flushed = batcher.append("q", 1, "c")

    assert flushed == b"abc"
    # Despues del flush automatico el buffer queda vacio.
    assert batcher.drain_client(1) == {}


def test_append_flushes_when_max_bytes_reached():
    batcher = OutputBatcher(_FakeSerializer(), max_bytes=4, max_tx=100)

    assert batcher.append("q", 1, "aa") is None
    flushed = batcher.append("q", 1, "bb")

    assert flushed == b"aabb"


def test_drain_client_returns_partial_buffers():
    batcher = OutputBatcher(_FakeSerializer(), max_bytes=1024, max_tx=100)

    batcher.append("q_a", 1, "x")
    batcher.append("q_a", 1, "y")
    batcher.append("q_b", 1, "z")

    drained = batcher.drain_client(1)

    assert drained == {"q_a": b"xy", "q_b": b"z"}
    # Drenar vacia los buffers del cliente.
    assert batcher.drain_client(1) == {}


def test_buffers_are_isolated_per_client():
    batcher = OutputBatcher(_FakeSerializer(), max_bytes=1024, max_tx=100)

    batcher.append("q", 1, "a")
    batcher.append("q", 2, "b")

    # drain_client(1) no toca el buffer del cliente 2.
    assert batcher.drain_client(1) == {"q": b"a"}
    assert batcher.drain_client(2) == {"q": b"b"}


def test_discard_client_removes_without_returning():
    batcher = OutputBatcher(_FakeSerializer(), max_bytes=1024, max_tx=100)

    batcher.append("q", 1, "x")
    batcher.discard_client(1)

    assert batcher.drain_client(1) == {}
