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

    # WAL API: a worker needs the flushed batches before committing (to stamp
    # them into the outbox) and must reproduce the same transition in
    # apply_change, live and on replay. plan_* compute without mutating, apply_*
    # mutate the same way; both share _simulate_append so they can't diverge.

    @staticmethod
    def _simulate_append(
        buf: list[bytes], buf_bytes: int, payloads: list[bytes],
        max_items: int, max_bytes: int,
    ) -> tuple[list[bytes], list[bytes], int]:
        """Append ``payloads`` to (``buf``, ``buf_bytes``), flushing whenever a
        limit is reached. Pure: operates on copies. Returns
        (flushed_batches, remaining_buf, remaining_bytes)."""
        flushed: list[bytes] = []
        cur = list(buf)
        cur_bytes = buf_bytes
        for payload in payloads:
            cur.append(payload)
            cur_bytes += len(payload)
            if len(cur) >= max_items or cur_bytes >= max_bytes:
                flushed.append(b"".join(cur))
                cur = []
                cur_bytes = 0
        return flushed, cur, cur_bytes

    def plan_append(self, key: Hashable, payloads: list[bytes]) -> list[bytes]:
        """Batches that would flush if ``payloads`` were appended under ``key``,
        without mutating (read by business_fn to stamp outputs before commit)."""
        with self._lock:
            flushed, _, _ = self._simulate_append(
                self._buffers.get(key, []), self._bytes_by_key.get(key, 0),
                payloads, self._max_items, self._max_bytes,
            )
        return flushed

    def apply_append(self, key: Hashable, payloads: list[bytes]) -> None:
        """Append ``payloads`` under ``key`` and drop whatever flushed (those
        batches already live in the outbox). The sole buffer mutator for data;
        run live and on WAL replay, it matches plan_append exactly."""
        with self._lock:
            _, rem, rem_bytes = self._simulate_append(
                self._buffers.get(key, []), self._bytes_by_key.get(key, 0),
                payloads, self._max_items, self._max_bytes,
            )
            if rem:
                self._buffers[key] = rem
                self._bytes_by_key[key] = rem_bytes
            else:
                self._buffers.pop(key, None)
                self._bytes_by_key.pop(key, None)

    def plan_drain(
        self, predicate: Callable[[Hashable], bool]
    ) -> list[tuple[Hashable, bytes]]:
        """The (key, batch) pairs a drain would emit, without mutating (read by
        business_fn at EOF/flush before the close change is committed)."""
        with self._lock:
            return [
                (key, b"".join(self._buffers[key]))
                for key in self._buffers
                if predicate(key) and self._buffers[key]
            ]

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
