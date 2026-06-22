import pytest

from common.eof_coordinator import EofCoordinator
from workers.file_ingestor.file_ingestor_state import FileIngestorState


def _coordinator() -> EofCoordinator:
    return EofCoordinator(
        instance_id=0,
        total_instances=3,
        control_queue_prefix="ctrl",
        response_queue_prefix="resp",
        mode="broadcast",
    )


def test_apply_data_accumulates_per_client():
    state = FileIngestorState(_coordinator())
    state.apply_change(FileIngestorState.data_change(7, 3))
    state.apply_change(FileIngestorState.data_change(7, 2))
    state.apply_change(FileIngestorState.data_change(8, 4))

    assert state.processed_count(7) == 5
    assert state.processed_count(8) == 4
    assert state.processed_count(99) == 0


def test_close_marks_closed_and_drops_count():
    state = FileIngestorState(_coordinator())
    state.apply_change(FileIngestorState.data_change(7, 3))
    state.apply_change(FileIngestorState.close_change(7))
    assert state.processed_count(7) == 0
    assert state.is_closed(7)


def test_closed_client_ignores_late_data():
    state = FileIngestorState(_coordinator())
    state.apply_change(FileIngestorState.close_change(7))
    state.apply_change(FileIngestorState.data_change(7, 5))  # late straggler
    assert state.processed_count(7) == 0


def test_compound_change_applies_all_subchanges():
    state = FileIngestorState(_coordinator())
    state.apply_change(FileIngestorState.data_change(7, 4))
    state.apply_change(
        FileIngestorState.compound_change(
            FileIngestorState.coordinator_cleanup_change(7),
            FileIngestorState.close_change(7),
        )
    )
    assert state.is_closed(7)
    assert state.processed_count(7) == 0


def test_snapshot_restore_roundtrip_includes_coordinator():
    coordinator = _coordinator()
    coordinator._leader_expected[7] = 42
    state = FileIngestorState(coordinator)
    state.apply_change(FileIngestorState.data_change(7, 5))

    snapshot = state.snapshot()

    restored_coordinator = _coordinator()
    restored = FileIngestorState(restored_coordinator)
    restored.restore(snapshot)

    assert restored.processed_count(7) == 5
    assert restored_coordinator._leader_expected[7] == 42


def test_restore_empty_is_fresh():
    state = FileIngestorState(_coordinator())
    state.apply_change(FileIngestorState.data_change(7, 5))
    state.restore({})
    assert state.processed_count(7) == 0


def test_unknown_change_type_raises():
    state = FileIngestorState(_coordinator())
    with pytest.raises(ValueError):
        state.apply_change({"type": "nonsense", "client_id": 1})
