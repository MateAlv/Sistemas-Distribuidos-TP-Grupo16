import threading
from typing import Protocol

from common.domain.transaction import Transaction


class TransactionSerializer(Protocol):
    def serialize(self, tx: Transaction) -> bytes:
        ...


class OutputBatcher:
    def __init__(
        self, serializer: TransactionSerializer, max_bytes: int, max_tx: int
    ) -> None:
        self._serializer = serializer
        self._max_bytes = max_bytes
        self._max_tx = max_tx
        self._buffers: dict[tuple[str, int], list[bytes]] = {}
        self._bytes_by_key: dict[tuple[str, int], int] = {}
        self._lock = threading.Lock()

    def append(
        self, queue_name: str, client_id: int, transaction: Transaction
    ) -> bytes | None:
        payload = self._serializer.serialize(transaction)
        key = (queue_name, client_id)
        with self._lock:
            buf = self._buffers.setdefault(key, [])
            buf.append(payload)
            new_bytes = self._bytes_by_key.get(key, 0) + len(payload)
            self._bytes_by_key[key] = new_bytes
            if len(buf) >= self._max_tx or new_bytes >= self._max_bytes:
                return self._drain_locked(key)
        return None

    def drain_client(self, client_id: int) -> dict[str, bytes]:
        out: dict[str, bytes] = {}
        with self._lock:
            keys = [key for key in self._buffers if key[1] == client_id]
            for key in keys:
                drained = self._drain_locked(key)
                if drained is not None:
                    queue_name, _ = key
                    out[queue_name] = drained
        return out

    def discard_client(self, client_id: int) -> None:
        with self._lock:
            for key in [k for k in self._buffers if k[1] == client_id]:
                self._buffers.pop(key, None)
                self._bytes_by_key.pop(key, None)

    def _drain_locked(self, key: tuple[str, int]) -> bytes | None:
        buf = self._buffers.pop(key, None)
        self._bytes_by_key.pop(key, None)
        if not buf:
            return None
        return b"".join(buf)
