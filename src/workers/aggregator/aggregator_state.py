"""WorkerState adapter for the aggregator.

Wraps the aggregator's mutable state -- per-client reduction processors, the
per-client data counters, the closed-client set and the EOF coordinator --
behind the snapshot/restore/apply_change contract the durable-state engine
expects.

apply_change is the single mutation path of the data plane: it runs live when a
new DATA message is processed and again, verbatim, when the WAL is replayed on
recovery. The raw input payload travels inside the change (base64-encoded, since
the WAL frames state changes as JSON) so that parsing and accumulation stay on
this one path and reproduce identically on replay.
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
        self._configuration = configuration
        self._coordinator = coordinator
        self._processor_factory = processor_factory
        self._data_count_by_client: dict[int, int] = {}
        self._processors_by_client: dict = {}
        self._closed_by_client: set[int] = set()

    @staticmethod
    def data_change(client_id: int, payload: bytes) -> dict:
        return {
            "client_id": client_id,
            "payload": base64.b64encode(payload).decode("ascii"),
        }

    def snapshot(self) -> dict:
        return {
            "data_count_by_client": dict(self._data_count_by_client),
            "processors_by_client": dict(self._processors_by_client),
            "closed_by_client": set(self._closed_by_client),
            "eof_coordinator": self._coordinator.snapshot(),
        }

    def restore(self, data: dict) -> None:
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
        client_id = change["client_id"]
        if client_id in self._closed_by_client:
            return
        payload = base64.b64decode(change["payload"])
        self._processor_for_client(client_id).accept(payload)
        self._data_count_by_client[client_id] = (
            self._data_count_by_client.get(client_id, 0) + 1
        )

    def data_count(self, client_id: int) -> int:
        return self._data_count_by_client.get(client_id, 0)

    def _processor_for_client(self, client_id: int):
        return self._processors_by_client.setdefault(
            client_id, self._processor_factory(self._configuration)
        )
