import threading
from typing import Callable, Hashable


class BatchBuffer:
    """Coalesces many small serialized payloads into fewer large batches before
    they are published to the middleware. Payloads are grouped by an arbitrary
    hashable key; a key's buffer is flushed once it reaches ``max_items`` or
    ``max_bytes``. Callers decide what the key means (e.g. an output queue, or a
    (partition, tag) pair) and how to map keys back to a destination on drain.

    Thread-safe: a single lock guards all buffers, so producers on different
    threads can append and drain concurrently.
    """

    def __init__(self, max_bytes: int, max_items: int) -> None:
        self._max_bytes = max_bytes
        self._max_items = max_items
        self._buffers: dict[Hashable, list[bytes]] = {}
        self._bytes_by_key: dict[Hashable, int] = {}
        self._lock = threading.Lock()

    def append(self, key: Hashable, payload: bytes) -> bytes | None:
        """Buffer one payload under ``key``. Returns the joined batch payload
        when the buffer reached a limit and was flushed, otherwise None."""
        with self._lock:
            buf = self._buffers.setdefault(key, [])
            buf.append(payload)
            new_bytes = self._bytes_by_key.get(key, 0) + len(payload)
            self._bytes_by_key[key] = new_bytes
            if len(buf) >= self._max_items or new_bytes >= self._max_bytes:
                return self._flush_key_locked(key)
        return None

    def flush(
        self, predicate: Callable[[Hashable], bool]
    ) -> list[tuple[Hashable, bytes]]:
        """Empty every buffer whose key matches ``predicate`` and return the
        (key, payload) pairs for the caller to publish."""
        out: list[tuple[Hashable, bytes]] = []
        with self._lock:
            for key in [k for k in self._buffers if predicate(k)]:
                payload = self._flush_key_locked(key)
                if payload is not None:
                    out.append((key, payload))
        return out

    def discard(self, predicate: Callable[[Hashable], bool]) -> None:
        """Drop every buffer whose key matches ``predicate`` without returning
        it (used when a client's resources are released)."""
        with self._lock:
            for key in [k for k in self._buffers if predicate(k)]:
                self._buffers.pop(key, None)
                self._bytes_by_key.pop(key, None)

    def snapshot(self) -> dict:
        """Return a picklable copy of all buffered data."""
        with self._lock:
            return {
                "buffers":      {k: list(v) for k, v in self._buffers.items()},
                "bytes_by_key": dict(self._bytes_by_key),
            }

    def restore(self, snap: dict) -> None:
        """Restore buffer state from a snapshot dict."""
        with self._lock:
            self._buffers      = {k: list(v) for k, v in snap["buffers"].items()}
            self._bytes_by_key = dict(snap["bytes_by_key"])

    def _flush_key_locked(self, key: Hashable) -> bytes | None:
        """Remove one key's buffer and its byte counter, returning that key's
        payloads concatenated into a single batch (or None if it was empty).
        Caller must hold ``self._lock`` (the ``_locked`` suffix)."""
        buf = self._buffers.pop(key, None)
        self._bytes_by_key.pop(key, None)
        if not buf:
            return None
        return b"".join(buf)
