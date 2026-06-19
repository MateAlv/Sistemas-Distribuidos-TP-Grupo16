"""WorkerState adapter for the aggregator.

Wraps the aggregator's mutable state -- per-client reduction processors, the
per-client data counters, the closed-client set and the EOF coordinator --
behind the snapshot/restore/apply_change contract the durable-state engine
expects.

apply_change is the single mutation path: it runs live when a message is
processed and again, verbatim, when the WAL is replayed on recovery. There are
two kinds of change, both treated as ordinary inputs by the handler:

  - "data": a DATA message. The raw payload travels inside the change
    (base64-encoded, since the WAL frames state changes as JSON) so that parsing
    and accumulation stay on this one path and reproduce identically on replay.
  - "close": an EOF flush. The client's aggregated results are emitted as outbox
    outputs (computed live, before the change is applied) and the client is then
    closed and dropped. Replay only re-applies the close; the results are not
    recomputed, they are republished from the durable outbox.
"""

from __future__ import annotations

import base64
from collections.abc import Callable

from common.eof_coordinator import EofCoordinator

ProcessorFactory = Callable[[str], object]


class AggregatorState:
    def __init__(
        self,
        configuration: str,
        coordinator: EofCoordinator,
        processor_factory: ProcessorFactory,
    ) -> None:
        # configuration picks the processor kind; coordinator is shared with the
        # worker and snapshotted alongside the per-client maps below.
        self._configuration = configuration
        self._coordinator = coordinator
        self._processor_factory = processor_factory
        self._data_count_by_client: dict[int, int] = {}
        self._processors_by_client: dict = {}
        self._closed_by_client: set[int] = set()

    @staticmethod
    def data_change(client_id: int, payload: bytes) -> dict:
        # WAL-serializable description of a DATA message; payload is base64 so it
        # survives the JSON framing and can be re-accepted verbatim on replay.
        return {
            "type": "data",
            "client_id": client_id,
            "payload": base64.b64encode(payload).decode("ascii"),
        }

    @staticmethod
    def close_change(client_id: int) -> dict:
        # An EOF flush carries no payload: the results live in the outbox, this
        # only records that the client must be closed and dropped.
        return {"type": "close", "client_id": client_id}

    def results_for(self, client_id: int) -> list[bytes]:
        # Aggregated outputs to emit at EOF; read live, before the close drops
        # the processor.
        processor = self._processors_by_client.get(client_id)
        return processor.results() if processor is not None else []

    def snapshot(self) -> dict:
        # Full picklable state for the LastState snapshot (processors included).
        return {
            "data_count_by_client": dict(self._data_count_by_client),
            "processors_by_client": dict(self._processors_by_client),
            "closed_by_client": set(self._closed_by_client),
            "eof_coordinator": self._coordinator.snapshot(),
        }

    def restore(self, data: dict) -> None:
        # Reload from a snapshot; an empty dict means a fresh, never-snapshotted
        # worker.
        if not data:
            self._data_count_by_client = {}
            self._processors_by_client = {}
            self._closed_by_client = set()
            return
        self._data_count_by_client = dict(data["data_count_by_client"])
        self._processors_by_client = dict(data["processors_by_client"])
        self._closed_by_client = set(data["closed_by_client"])
        self._coordinator.restore(data["eof_coordinator"])

    def apply_change(self, change: dict) -> None:
        # Single mutation path, run live and on replay; dispatch by change kind.
        kind = change["type"]
        client_id = change["client_id"]
        if kind == "data":
            self._apply_data(client_id, change)
        elif kind == "close":
            self._apply_close(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    def _apply_data(self, client_id: int, change: dict) -> None:
        # Accumulate one DATA message; a closed client ignores late stragglers.
        if client_id in self._closed_by_client:
            return
        payload = base64.b64decode(change["payload"])
        self._processor_for_client(client_id).accept(payload)
        self._data_count_by_client[client_id] = (
            self._data_count_by_client.get(client_id, 0) + 1
        )

    def _apply_close(self, client_id: int) -> None:
        # Close the client and free its processor/counter; idempotent on replay.
        self._closed_by_client.add(client_id)
        self._processors_by_client.pop(client_id, None)
        self._data_count_by_client.pop(client_id, None)

    def data_count(self, client_id: int) -> int:
        # Messages accumulated for a client so far (0 once closed).
        return self._data_count_by_client.get(client_id, 0)

    def _processor_for_client(self, client_id: int):
        # Lazily create the per-client reduction processor on first DATA.
        return self._processors_by_client.setdefault(
            client_id, self._processor_factory(self._configuration)
        )
