import csv
from dataclasses import dataclass


@dataclass
class LineSplitter:
    """Splits ordered byte chunks into raw newline-delimited lines."""

    max_line_bytes: int
    expected_offset: int = 0
    pending: bytes = b""

    def __post_init__(self) -> None:
        if self.max_line_bytes <= 0:
            raise ValueError("max_line_bytes must be greater than 0")

    def push(self, offset: int, payload: bytes) -> list[bytes]:
        if offset != self.expected_offset:
            raise ValueError(
                "unexpected chunk offset "
                f"(expected={self.expected_offset}, received={offset})"
            )

        lines = (self.pending + payload).split(b"\n")
        complete_lines = lines[:-1]
        pending = lines[-1]

        for line in complete_lines:
            _validate_line_size(line, self.max_line_bytes)
        _validate_line_size(pending, self.max_line_bytes)

        self.pending = pending
        self.expected_offset += len(payload)
        return complete_lines

    def finish(self) -> list[bytes]:
        if not self.pending:
            return []

        line = self.pending
        _validate_line_size(line, self.max_line_bytes)
        self.pending = b""
        return [line]

    def pending_size(self) -> int:
        return len(self.pending)


def _validate_line_size(line: bytes, max_line_bytes: int) -> None:
    if len(line) > max_line_bytes:
        raise ValueError(f"line exceeded max_line_bytes={max_line_bytes}")


def parse_csv_line(line: bytes) -> list[str]:
    rows = list(csv.reader([line.decode("utf-8")]))
    if len(rows) != 1:
        raise ValueError("expected exactly one CSV row")
    return rows[0]
