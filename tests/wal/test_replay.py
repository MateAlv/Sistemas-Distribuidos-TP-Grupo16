from pathlib import Path

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.fault_tolerance.wal.input_applied import InputApplied
from common.fault_tolerance.wal.input_done import InputDone
from common.fault_tolerance.wal.replay import ReplayResult, WALReplayer, apply_replay_record
from common.fault_tolerance.wal.wal import Wal


def _no_fsync(_fd: int) -> None:
    pass


def _wal(state_dir: Path) -> Wal:
    return Wal(state_dir, fsync_fn=_no_fsync)


def _entry(input_id: str, index: int = 0) -> OutboxEntry:
    return OutboxEntry(
        output_id=f"{input_id}#{index}",
        input_id=input_id,
        destination="next.queue",
        body=f"payload-{index}".encode(),
    )


def _applied(msg_id: str, seq: int, outputs: list[OutboxEntry] | None = None):
    return InputApplied(
        msg_id=msg_id,
        client_id=1,
        sender_id=2,
        seq=seq,
        state_change={"seq": seq},
        outputs=outputs if outputs is not None else [_entry(msg_id)],
    )


def test_replay_keeps_applied_input_pending_for_republish(tmp_path: Path) -> None:
    wal = _wal(tmp_path)
    applied = _applied("input-1", 1)
    wal.append(applied)
    wal.close()

    result = WALReplayer(_wal(tmp_path)).replay(checkpoint_lsn=-1)

    assert result.state_changes == [{"seq": 1}]
    assert result.applied_inputs == {"input-1": applied}
    assert result.done_inputs == {}
    assert result.pending_outbox == {"input-1": applied.outputs}
    assert result.outbox_to_republish() == applied.outputs


def test_replay_done_clears_applied_and_outbox(tmp_path: Path) -> None:
    wal = _wal(tmp_path)
    applied = _applied("input-1", 1)
    done = InputDone("input-1", 1, 2, 1)
    wal.append(applied)
    wal.append(done)
    wal.close()

    result = WALReplayer(_wal(tmp_path)).replay(checkpoint_lsn=-1)

    assert result.state_changes == [{"seq": 1}]
    assert result.applied_inputs == {}
    assert result.pending_outbox == {}
    assert result.done_inputs == {"input-1": done}
    assert result.outbox_to_republish() == []


def test_replay_preserves_state_change_order(tmp_path: Path) -> None:
    wal = _wal(tmp_path)
    wal.append(_applied("input-1", 1))
    wal.append(_applied("input-2", 2, outputs=[]))
    wal.close()

    result = WALReplayer(_wal(tmp_path)).replay(checkpoint_lsn=-1)

    assert result.state_changes == [{"seq": 1}, {"seq": 2}]


def test_replay_starts_after_checkpoint_lsn(tmp_path: Path) -> None:
    wal = _wal(tmp_path)
    first_lsn = wal.append(_applied("input-1", 1))
    wal.append(_applied("input-2", 2))
    wal.close()

    result = WALReplayer(_wal(tmp_path)).replay(checkpoint_lsn=first_lsn)

    assert set(result.applied_inputs) == {"input-2"}
    assert result.state_changes == [{"seq": 2}]


def test_apply_replay_record_is_idempotent_for_done_without_applied() -> None:
    result = ReplayResult()
    done = InputDone("input-1", 1, 2, 1)

    apply_replay_record(result, done)

    assert result.done_inputs == {"input-1": done}
    assert result.applied_inputs == {}
    assert result.pending_outbox == {}
