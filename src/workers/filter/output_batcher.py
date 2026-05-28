from typing import Protocol

from common.batch_buffer import BatchBuffer
from common.domain.transaction import Transaction


class TransactionSerializer(Protocol):
    def serialize(self, tx: Transaction) -> bytes:
        ...


class OutputBatcher:
    """Batches outgoing transactions per (queue_name, client_id) on top of the
    generic BatchBuffer, serializing each transaction as it is appended."""

    def __init__(
        self, serializer: TransactionSerializer, max_bytes: int, max_tx: int
    ) -> None:
        self._serializer = serializer
        self._buffer = BatchBuffer(max_bytes, max_tx)

    def append(
        self, queue_name: str, client_id: int, transaction: Transaction
    ) -> bytes | None:
        return self._buffer.append(
            (queue_name, client_id), self._serializer.serialize(transaction)
        )

    def drain_client(self, client_id: int) -> dict[str, bytes]:
        return {
            key[0]: payload
            for key, payload in self._buffer.flush(lambda k: k[1] == client_id)
        }

    def discard_client(self, client_id: int) -> None:
        self._buffer.discard(lambda k: k[1] == client_id)
