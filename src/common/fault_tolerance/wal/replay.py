from __future__ import annotations

from dataclasses import dataclass, field

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.fault_tolerance.wal.input_applied import InputApplied
from common.fault_tolerance.wal.input_done import InputDone
from common.fault_tolerance.wal.wal import Wal
from common.fault_tolerance.wal.wal_record import WalRecord


@dataclass
class ReplayResult:
    state_changes: list[dict] = field(default_factory=list)
    applied_inputs: dict[str, InputApplied] = field(default_factory=dict)
    done_inputs: dict[str, InputDone] = field(default_factory=dict)
    pending_outbox: dict[str, list[OutboxEntry]] = field(default_factory=dict)

    def outbox_to_republish(self) -> list[OutboxEntry]:
        return [
            entry
            for entries in self.pending_outbox.values()
            for entry in entries
        ]


class WALReplayer:
    def __init__(self, wal: Wal) -> None:
        self._wal = wal

    def replay(self, checkpoint_lsn: int) -> ReplayResult:
        result = ReplayResult()
        for record in self._wal.replay(checkpoint_lsn):
            apply_replay_record(result, record)
        return result


def apply_replay_record(result: ReplayResult, record: WalRecord) -> None:
    if isinstance(record, InputApplied):
        result.state_changes.append(record.state_change)
        result.applied_inputs[record.msg_id] = record
        result.pending_outbox[record.msg_id] = list(record.outputs)
        return

    if isinstance(record, InputDone):
        result.done_inputs[record.msg_id] = record
        result.applied_inputs.pop(record.msg_id, None)
        result.pending_outbox.pop(record.msg_id, None)
        return

    raise TypeError(f"unsupported replay record type: {type(record).__name__}")
