class UpstreamEofCounter:
    """Counts EOFs from N distinct upstream senders per client.

    Returns True from on_eof() on the Nth unique sender, signalling that the
    caller can flush that client. Not thread-safe — caller must hold its own lock.
    """

    def __init__(self, expected: int):
        self._expected = expected
        self._eofs_by_client: dict[int, set[int]] = {}
        self._closed: set[int] = set()

    def on_eof(self, client_id: int, sender_id: int) -> bool:
        """Record one EOF. Returns True iff this is the Nth unique sender."""
        if client_id in self._closed:
            return False
        seen = self._eofs_by_client.setdefault(client_id, set())
        if sender_id in seen:
            return False
        seen.add(sender_id)
        return len(seen) >= self._expected

    def close(self, client_id: int) -> None:
        self._eofs_by_client.pop(client_id, None)
        self._closed.add(client_id)

    def count(self, client_id: int) -> int:
        return len(self._eofs_by_client.get(client_id, ()))

    def snapshot(self) -> dict:
        """Return a picklable copy of counter state. Caller must hold the worker's lock."""
        return {
            "eofs_by_client": {k: set(v) for k, v in self._eofs_by_client.items()},
            "closed":         set(self._closed),
        }

    def restore(self, snap: dict) -> None:
        """Restore state from a snapshot dict. Caller must hold the worker's lock."""
        self._eofs_by_client = snap["eofs_by_client"]
        self._closed         = snap["closed"]
