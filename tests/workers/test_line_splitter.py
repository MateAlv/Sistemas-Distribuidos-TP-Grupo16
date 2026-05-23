import pytest

from workers.file_splitter.line_splitter import LineSplitter


def test_line_splitter_stitches_lines_across_chunks():
    splitter = LineSplitter(max_line_bytes=32)

    assert splitter.push(0, b"a,b\npar") == [b"a,b"]
    assert splitter.pending_size() == 3
    assert splitter.push(7, b"tial\nlast") == [b"partial"]
    assert splitter.pending_size() == 4
    assert splitter.finish() == [b"last"]
    assert splitter.finish() == []


def test_line_splitter_rejects_unexpected_offset():
    splitter = LineSplitter(max_line_bytes=32)

    with pytest.raises(ValueError, match="unexpected chunk offset"):
        splitter.push(1, b"a,b\n")


def test_line_splitter_returns_raw_lines():
    splitter = LineSplitter(max_line_bytes=32)

    assert splitter.push(0, b"a,b\r\n\n") == [b"a,b\r", b""]
    assert splitter.finish() == []


def test_line_splitter_rejects_oversized_pending_line():
    splitter = LineSplitter(max_line_bytes=3)

    with pytest.raises(ValueError, match="line exceeded max_line_bytes=3"):
        splitter.push(0, b"abcd")


def test_line_splitter_rejects_oversized_complete_line():
    splitter = LineSplitter(max_line_bytes=3)

    with pytest.raises(ValueError, match="line exceeded max_line_bytes=3"):
        splitter.push(0, b"abcd\n")


def test_line_splitter_handles_empty_file_chunk():
    splitter = LineSplitter(max_line_bytes=32)

    assert splitter.push(0, b"") == []
    assert splitter.finish() == []
    assert splitter.expected_offset == 0
