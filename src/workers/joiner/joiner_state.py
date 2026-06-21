"""WorkerState adapter for the joiner worker.

Wraps the joiner's mutable per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

Change types
------------
  "data"
      A DATA message arrived. Carries the raw payload (base64) so apply_change
      can replay it through the processor and keep the accumulated reduction
      state identical to what the live worker produced.
  "eof"
      One upstream EOF was received. apply_change increments the per-client EOF
      counter. The emit decision (when count == AGGREGATION_AMOUNT) belongs to
      the live worker; apply_change only tracks the counter.
  "close"
      The client was flushed and cleaned up after all AGGREGATION_AMOUNT EOFs
      arrived. Drops the processor and counters and marks the client closed so
      late-arriving messages are ignored on replay.

No EofCoordinator: this worker uses a simple per-client EOF counter.

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  DATA message
    → data_change(client_id, payload)

  Upstream EOF received (one of AGGREGATION_AMOUNT sources):
    → eof_change(client_id)
    If _eof_count_by_client[client_id] == AGGREGATION_AMOUNT after applying:
      Read processor results via processor interface.
      Emit aggregated results to outbox (to downstream aggregator).
      → close_change(client_id)

State accessors (read before close_change)
------------------------------------------
  data_count(client_id) → int  DATA messages accumulated so far
  eof_count_by_client   → dict[int, int]  (internal; check directly for emit decision)
"""

from __future__ import annotations

import base64
from collections.abc import Callable


ProcessorFactory = Callable[[], object]


class JoinerState:
    def __init__(self, processor_factory: ProcessorFactory) -> None:
        # Factory is called once per client on first DATA message; it must return
        # a fresh processor of the right type for this joiner's CONFIGURATION.
        self._processor_factory = processor_factory
        self._processors_by_client: dict[int, object] = {}
        self._data_count_by_client: dict[int, int] = {}
        self._eof_count_by_client: dict[int, int] = {}
        # Set is sufficient here; the live worker stores timestamps for LRU
        # cleanup, but the adapter only needs closed/not-closed.
        self._closed_by_client: set[int] = set()

    @staticmethod
    def data_change(client_id: int, payload: bytes) -> dict:
        # payload is base64 because the WAL frames state_change dicts as JSON.
        return {
            "type": "data",
            "client_id": client_id,
            "payload_b64": base64.b64encode(payload).decode("ascii"),
        }

    @staticmethod
    def eof_change(client_id: int) -> dict:
        return {"type": "eof", "client_id": client_id}

    @staticmethod
    def close_change(client_id: int) -> dict:
        return {"type": "close", "client_id": client_id}

    def data_count(self, client_id: int) -> int:
        return self._data_count_by_client.get(client_id, 0)

    def eof_count(self, client_id: int) -> int:
        return self._eof_count_by_client.get(client_id, 0)

    def results_for(self, client_id: int) -> list[bytes]:
        # Read live, before the close_change drops the processor.
        processor = self._processors_by_client.get(client_id)
        return processor.results() if processor is not None else []

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        # Processors (Q2/Q3/Q5JoinerProcessor) hold only dicts, lists and
        # primitives — all picklable without custom serialization.
        return {
            "processors_by_client": dict(self._processors_by_client),
            "data_count_by_client": dict(self._data_count_by_client),
            "eof_count_by_client": dict(self._eof_count_by_client),
            "closed_by_client": set(self._closed_by_client),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._processors_by_client = {}
            self._data_count_by_client = {}
            self._eof_count_by_client = {}
            self._closed_by_client = set()
            return
        self._processors_by_client = dict(data["processors_by_client"])
        self._data_count_by_client = dict(data["data_count_by_client"])
        self._eof_count_by_client = dict(data["eof_count_by_client"])
        self._closed_by_client = set(data["closed_by_client"])

    def apply_change(self, change: dict) -> None:
        # Single mutation path — runs both live and during WAL replay.
        kind = change["type"]
        client_id = change["client_id"]
        if kind == "data":
            self._apply_data(client_id, change)
        elif kind == "eof":
            self._apply_eof(client_id)
        elif kind == "close":
            self._apply_close(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    def _apply_data(self, client_id: int, change: dict) -> None:
        # Closed clients are ignored; late DATA messages on WAL replay must not
        # corrupt the processor of a subsequently re-opened client.
        if client_id in self._closed_by_client:
            return
        payload = base64.b64decode(change["payload_b64"])
        self._processor_for(client_id).accept(payload)
        self._data_count_by_client[client_id] = (
            self._data_count_by_client.get(client_id, 0) + 1
        )

    def _apply_eof(self, client_id: int) -> None:
        if client_id in self._closed_by_client:
            return
        self._eof_count_by_client[client_id] = (
            self._eof_count_by_client.get(client_id, 0) + 1
        )

    def _apply_close(self, client_id: int) -> None:
        # Idempotent: re-closing an already-closed client is a no-op.
        self._processors_by_client.pop(client_id, None)
        self._data_count_by_client.pop(client_id, None)
        self._eof_count_by_client.pop(client_id, None)
        self._closed_by_client.add(client_id)

    def _processor_for(self, client_id: int):
        return self._processors_by_client.setdefault(
            client_id, self._processor_factory()
        )
