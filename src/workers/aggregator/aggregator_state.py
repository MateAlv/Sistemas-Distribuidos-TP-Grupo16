"""WorkerState adapter for the aggregator.

Wraps the aggregator's mutable state — per-client reduction processors, data
counters, the closed-client set and the EofCoordinator — behind the
snapshot/restore/apply_change contract the durable-state engine expects.

apply_change is the single mutation path: runs live when a message is processed
and verbatim again during WAL replay on recovery.

Change types
------------
  "data"
      A DATA message arrived. The raw payload is base64-encoded (WAL is JSON)
      so that deserialization + accumulation replay identically.
  "close"
      The client was flushed and cleaned up. Results are computed live via
      results_for() and placed in the outbox BEFORE this change is applied;
      replay only re-closes, not re-computes.
  "coordinator_upstream_eof"
      coordinator.on_upstream_eof was called. apply_change replays the call;
      the returned action is discarded (no I/O on replay).
  "coordinator_msg"
      coordinator.process_control_message was called with a state-mutating
      type: EOF_RECEIVED, PROCESSED_ANSWER, or FLUSH_ACK (broadcast mode).
      FLUSH_ACK uses the standard path — _on_flush_ack handles coordinator
      leader-state cleanup internally.
  "coordinator_cleanup"
      coordinator.cleanup_client was called (non-leader, after receiving
      FLUSH_ORDER → FlushAction(is_leader=False)).

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  DATA message
    → data_change(client_id, payload)

  Upstream EOF received (data thread):
    action = coordinator.on_upstream_eof(client_id, expected_total, count, fwd)
    → coordinator_upstream_eof_change(client_id, expected_total, count, fwd)
    Execute action outside the lock (I/O).

  Control EOF_RECEIVED (broadcast, control thread):
    action = coordinator.process_control_message(EOF_RECEIVED, client_id, ctrl, count, fwd)
    → coordinator_msg_change(EOF_RECEIVED, client_id, ctrl.sender_id,
                              ctrl.expected_total, ctrl.processed_count, count, fwd)
    Execute action outside the lock.

  Response PROCESSED_ANSWER (leader, response thread):
    action = coordinator.process_control_message(PROCESSED_ANSWER, client_id, ctrl)
    → coordinator_msg_change(PROCESSED_ANSWER, client_id, ctrl.sender_id,
                              ctrl.expected_total, ctrl.processed_count)
    Execute action outside the lock.

  Non-leader close (FLUSH_ORDER received → FlushAction(is_leader=False)):
    coordinator.cleanup_client(client_id) must happen before business close.
    PersistentStateHandler.handle() supports one change per message — bundle
    both mutations into a single compound change dict, for example:
      {"type": "non_leader_close", "client_id": N}
    and handle it in apply_change as: cleanup_client → _apply_close.
    The two separate change constructors (coordinator_cleanup_change +
    close_change) are provided for direct apply_change sequences when the
    caller handles its own WAL framing.

  Leader close (final FLUSH_ACK → FlushAction(is_leader=True), or N=1):
    action = coordinator.process_control_message(FLUSH_ACK, client_id, ctrl)
    → coordinator_msg_change(FLUSH_ACK, client_id, ctrl.sender_id,
                              ctrl.expected_total, ctrl.processed_count)
    Read results_for(client_id) → emit to outbox.
    → close_change(client_id)
    (_on_flush_ack already cleaned coordinator state; no coordinator_cleanup needed.)

  N=1 shortcut (on_upstream_eof returns FlushAction directly):
    → coordinator_upstream_eof_change(...)
    Read results_for(client_id) → emit to outbox.
    → close_change(client_id)
"""

from __future__ import annotations

import base64
from collections.abc import Callable

from common.eof_coordinator import EofCoordinator
from common.message_protocol.internal.common import ControlMessage, MessageType

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
    def abort_change(client_id: int) -> dict:
        return AggregatorState.compound_change(
            AggregatorState.coordinator_cleanup_change(client_id),
            AggregatorState.close_change(client_id),
        )

    @staticmethod
    def coordinator_cleanup_change(client_id: int) -> dict:
        # Non-leader cleanup after FlushAction(is_leader=False).
        return {"type": "coordinator_cleanup", "client_id": client_id}

    @staticmethod
    def compound_change(*changes: dict) -> dict:
        # Bundle multiple change dicts into one so PersistentStateHandler sees a
        # single change per message (its one-change-per-handle() invariant).
        return {"type": "compound", "changes": list(changes)}

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

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
