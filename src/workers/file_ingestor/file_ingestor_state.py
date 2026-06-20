"""WorkerState adapter for the file ingestor.

Wraps the ingestor's mutable business state — per-client transaction counters,
the total-batches-consumed counter and the EOF coordinator — behind the
snapshot/restore/apply_change contract the durable-state engine expects.

Two kinds of change:
  - "data": a DATA batch was processed. The change carries the count of
    transactions forwarded (not the raw payload), so replay updates the counter
    deterministically without re-parsing. The actual transaction outputs live in
    the durable outbox and are re-published from there on recovery.
  - "close": the client was flushed downstream. Pops the per-client counter so
    any late-arriving messages for that client are treated as no-ops.
"""

from __future__ import annotations

from common.eof_coordinator import EofCoordinator


class FileIngestorState:
    def __init__(self, coordinator: EofCoordinator) -> None:
        self._coordinator = coordinator
        self._processed_by_client: dict[int, int] = {}
        self._batches_consumed: int = 0

    @staticmethod
    def data_change(client_id: int, transactions_forwarded: int) -> dict:
        """Build the WAL change dict for one processed DATA batch."""
        return {
            "type": "data",
            "client_id": client_id,
            "transactions_forwarded": transactions_forwarded,
        }

    @staticmethod
    def close_change(client_id: int) -> dict:
        """Build the WAL change dict for a client EOF flush."""
        return {"type": "close", "client_id": client_id}

    def processed_count(self, client_id: int) -> int:
        """Transactions forwarded so far for client_id (0 once closed)."""
        return self._processed_by_client.get(client_id, 0)

    def batches_consumed(self) -> int:
        return self._batches_consumed

    def snapshot(self) -> dict:
        return {
            "processed_by_client": dict(self._processed_by_client),
            "batches_consumed": self._batches_consumed,
            "eof_coordinator": self._coordinator.snapshot(),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._processed_by_client = {}
            self._batches_consumed = 0
            return
        self._processed_by_client = dict(data["processed_by_client"])
        self._batches_consumed = data["batches_consumed"]
        self._coordinator.restore(data["eof_coordinator"])

    def apply_change(self, change: dict) -> None:
        """Single mutation path — runs both live and during WAL replay."""
        kind = change["type"]
        client_id = change["client_id"]
        if kind == "data":
            self._apply_data(client_id, change)
        elif kind == "close":
            self._apply_close(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    def _apply_data(self, client_id: int, change: dict) -> None:
        forwarded = change["transactions_forwarded"]
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0) + forwarded
        )
        self._batches_consumed += 1

    def _apply_close(self, client_id: int) -> None:
        self._processed_by_client.pop(client_id, None)
