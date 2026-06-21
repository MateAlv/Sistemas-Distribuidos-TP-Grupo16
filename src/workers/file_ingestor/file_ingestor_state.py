"""WorkerState adapter for the file ingestor.

Wraps the ingestor's durable business state behind the snapshot/restore/
apply_change contract. There are two pieces of state:

  - ``processed_by_client``: how many transactions this instance has forwarded
    downstream per client. The EOF coordinator reports this count, so it must be
    exact -- it is updated only through ``apply_change`` (the WAL-replayed path).
  - the ``EofCoordinator``: the inter-replica EOF protocol state. It is shared
    with the worker (the control/response threads call it directly) and rides
    along in the snapshot so a restart restores the in-flight EOF rounds.

Only DATA messages flow through ``apply_change`` (one change kind, ``"data"``).
EOF coordination is durable via the snapshot, not the WAL -- see the plan.
"""

from __future__ import annotations

from common.eof_coordinator import EofCoordinator


class FileIngestorState:
    def __init__(self, coordinator: EofCoordinator) -> None:
        self._coordinator = coordinator
        self._processed_by_client: dict[int, int] = {}

    @staticmethod
    def data_change(client_id: int, forwarded: int) -> dict:
        return {"type": "data", "client_id": client_id, "delta": forwarded}

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def drop_client(self, client_id: int) -> None:
        self._processed_by_client.pop(client_id, None)

    def snapshot(self) -> dict:
        return {
            "processed_by_client": dict(self._processed_by_client),
            "eof_coordinator": self._coordinator.snapshot(),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._processed_by_client = {}
            return
        self._processed_by_client = dict(data["processed_by_client"])
        self._coordinator.restore(data["eof_coordinator"])

    def apply_change(self, change: dict) -> None:
        kind = change["type"]
        if kind == "data":
            client_id = change["client_id"]
            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + change["delta"]
            )
        else:
            raise ValueError(f"unknown change type: {kind}")
