from pathlib import Path

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.fault_tolerance.wal.input_applied import InputApplied
from common.fault_tolerance.wal.input_done import InputDone
from common.fault_tolerance.wal.wal import Wal


def _no_fsync(_fd: int) -> None:
    pass


def _wal(state_dir: Path) -> Wal:
    return Wal(state_dir, fsync_fn=_no_fsync)


def _input_applied(msg_id: str, seq: int) -> InputApplied:
    return InputApplied(
        msg_id=msg_id,
        client_id=1,
        sender_id=2,
        seq=seq,
        state_change={"count": seq},
        outputs=[
            OutboxEntry(
                output_id=f"{msg_id}#0",
                input_id=msg_id,
                destination="next.queue",
                body=b"payload",
            )
        ],
    )


def test_append_and_replay_round_trip(tmp_path: Path) -> None:
    wal = _wal(tmp_path)
    applied = _input_applied("node:client:1", 1)
    done = InputDone("node:client:1", 1, 2, 1)

    wal.append(applied)
    wal.append(done)
    wal.close()

    recovered = list(_wal(tmp_path).replay(from_record=-1))

    assert recovered == [applied, done]


def test_replay_filters_records_at_or_before_checkpoint(tmp_path: Path) -> None:
    wal = _wal(tmp_path)
    first_lsn = wal.append(InputDone("first", 1, 2, 1))
    second = InputDone("second", 1, 2, 2)
    second_lsn = wal.append(second)
    wal.close()

    recovered = list(_wal(tmp_path).replay(from_record=first_lsn))

    assert recovered == [second]
    assert second_lsn > first_lsn


def test_current_record_number_returns_next_lsn(tmp_path: Path) -> None:
    wal = _wal(tmp_path)
    assert wal.current_record_number() == 0

    first_lsn = wal.append(InputDone("first", 1, 2, 1))
    next_lsn = wal.current_record_number()
    second_lsn = wal.append(InputDone("second", 1, 2, 2))
    wal.close()

    assert first_lsn == 0
    assert second_lsn == next_lsn


def test_reopens_existing_wal_and_appends_at_eof(tmp_path: Path) -> None:
    first = _wal(tmp_path)
    first.append(InputDone("first", 1, 2, 1))
    eof_lsn = first.current_record_number()
    first.close()

    second = _wal(tmp_path)
    second_lsn = second.append(InputDone("second", 1, 2, 2))
    second.close()

    assert second_lsn == eof_lsn
    assert [record.msg_id for record in _wal(tmp_path).replay(-1)] == [
        "first",
        "second",
    ]


def test_constructor_creates_state_directory(tmp_path: Path) -> None:
    state_dir = tmp_path / "nested" / "state"
    wal = _wal(state_dir)
    wal.append(InputDone("id", 1, 2, 1))
    wal.close()

    assert (state_dir / "wal.current").exists()


def test_rotate_starts_fresh_wal_and_preserves_previous(tmp_path: Path) -> None:
    wal = _wal(tmp_path)
    wal.append(InputDone("old", 1, 2, 1))

    wal.rotate()
    assert list(wal.replay(-1)) == []

    wal.append(InputDone("new", 1, 2, 2))
    wal.close()

    assert (tmp_path / "wal.previous").exists()
    assert [record.msg_id for record in _wal(tmp_path).replay(-1)] == ["new"]
