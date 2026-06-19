import pickle

from common.fault_tolerance.snapshot.last_state import LastState
from common.fault_tolerance.snapshot.snapshot import Snapshot


def _snap(value: int) -> Snapshot:
    return Snapshot(
        wal_checkpoint_record=-1,
        inbox={"applied": {}, "done": {}},
        outbox={},
        worker_state={"total": value},
    )


def test_load_without_snapshot_returns_none(tmp_path):
    assert LastState(tmp_path).load() is None


def test_commit_then_load_round_trip(tmp_path):
    store = LastState(tmp_path)
    store.commit(_snap(42))
    loaded = store.load()
    assert loaded.worker_state == {"total": 42}
    assert loaded.wal_checkpoint_record == -1


def test_second_commit_keeps_previous(tmp_path):
    store = LastState(tmp_path)
    store.commit(_snap(1))
    store.commit(_snap(2))
    assert (tmp_path / "last_state.current").exists()
    assert (tmp_path / "last_state.previous").exists()
    assert store.load().worker_state == {"total": 2}


def test_falls_back_to_previous_when_current_corrupt(tmp_path):
    store = LastState(tmp_path)
    store.commit(_snap(1))  # becomes previous
    store.commit(_snap(2))  # becomes current
    (tmp_path / "last_state.current").write_bytes(b"garbage not pickle")

    loaded = store.load()
    assert loaded.worker_state == {"total": 1}


def test_survives_new_instance(tmp_path):
    LastState(tmp_path).commit(_snap(7))
    assert LastState(tmp_path).load().worker_state == {"total": 7}


def test_tmp_file_is_not_left_behind(tmp_path):
    store = LastState(tmp_path)
    store.commit(_snap(1))
    assert not (tmp_path / "last_state.tmp").exists()
