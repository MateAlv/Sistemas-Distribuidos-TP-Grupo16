"""Per-client state for the file ingestor (broadcast EOF mode).

The EofCoordinator transitions are recorded as coordinator_* changes, so a crash
recovers the leader role and flush/close decisions, not just the last snapshot.
Count-collection messages (EOF_RECEIVED / PROCESSED_ANSWER / PROCESSED_REQUEST)
are transient and rebuilt by redelivery and the coordinator's retry.
"""

from __future__ import annotations

from common.eof_coordinator import EofCoordinator
from common.message_protocol.internal.common import ControlMessage, MessageType


class FileIngestorState:
    def __init__(self, coordinator: EofCoordinator) -> None:
        self._coordinator = coordinator
        self._processed_by_client: dict[int, int] = {}
        self._closed_by_client: set[int] = set()
        self._batches_consumed = 0

    @staticmethod
    def data_change(client_id: int, transactions_forwarded: int) -> dict:
        return {
            "type": "data",
            "client_id": client_id,
            "transactions_forwarded": transactions_forwarded,
        }

    @staticmethod
    def close_change(client_id: int) -> dict:
        return {"type": "close", "client_id": client_id}

    @staticmethod
    def coordinator_upstream_eof_change(
        client_id: int, expected_total: int, count: int, forwarded: int
    ) -> dict:
        return {
            "type": "coordinator_upstream_eof",
            "client_id": client_id,
            "expected_total": expected_total,
            "count": count,
            "forwarded": forwarded,
        }

    @staticmethod
    def coordinator_msg_change(
        msg_type: MessageType,
        client_id: int,
        sender_id: int,
        expected_total: int,
        processed_count: int,
        count: int = 0,
        forwarded: int = 0,
    ) -> dict:
        return {
            "type": "coordinator_msg",
            "msg_type": msg_type.value,
            "client_id": client_id,
            "sender_id": sender_id,
            "expected_total": expected_total,
            "processed_count": processed_count,
            "count": count,
            "forwarded": forwarded,
        }

    @staticmethod
    def coordinator_cleanup_change(client_id: int) -> dict:
        return {"type": "coordinator_cleanup", "client_id": client_id}

    @staticmethod
    def compound_change(*changes: dict) -> dict:
        return {"type": "compound", "changes": list(changes)}

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    def processed_count(self, client_id: int) -> int:
        return self._processed_by_client.get(client_id, 0)

    def batches_consumed(self) -> int:
        return self._batches_consumed

    def snapshot(self) -> dict:
        return {
            "processed_by_client": dict(self._processed_by_client),
            "closed_by_client": set(self._closed_by_client),
            "batches_consumed": self._batches_consumed,
            "eof_coordinator": self._coordinator.snapshot(),
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._processed_by_client = {}
            self._closed_by_client = set()
            self._batches_consumed = 0
            return
        self._processed_by_client = dict(data["processed_by_client"])
        self._closed_by_client = set(data["closed_by_client"])
        self._batches_consumed = data["batches_consumed"]
        self._coordinator.restore(data["eof_coordinator"])

    def apply_change(self, change: dict) -> None:
        kind = change["type"]
        if kind == "compound":
            for sub in change["changes"]:
                self.apply_change(sub)
            return
        client_id = change["client_id"]
        if kind == "data":
            self._apply_data(client_id, change)
        elif kind == "close":
            self._apply_close(client_id)
        elif kind == "coordinator_upstream_eof":
            self._coordinator.on_upstream_eof(
                client_id, change["expected_total"], change["count"], change["forwarded"]
            )
        elif kind == "coordinator_msg":
            ctrl = ControlMessage(
                sender_id=change["sender_id"],
                expected_total=change["expected_total"],
                processed_count=change["processed_count"],
            )
            self._coordinator.process_control_message(
                MessageType(change["msg_type"]),
                client_id, ctrl, change["count"], change["forwarded"],
            )
        elif kind == "coordinator_cleanup":
            self._coordinator.cleanup_client(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    def _apply_data(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        self._processed_by_client[client_id] = (
            self._processed_by_client.get(client_id, 0)
            + change["transactions_forwarded"]
        )
        self._batches_consumed += 1

    def _apply_close(self, client_id: int) -> None:
        self._closed_by_client.add(client_id)
        self._processed_by_client.pop(client_id, None)
