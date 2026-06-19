from common.constants import C_Q5
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import (
    PersistentStateHandler,
)
from common.fault_tolerance.outbox import OutboxEntry
from common.fault_tolerance.worker_state import WorkerState
from workers.aggregator.aggregator_state import AggregatorState
from workers.aggregator.processors import create_aggregator_processor

CLIENT = 7


class FakeCoordinator:
    def __init__(self) -> None:
        self.state = {"seen": []}

    def snapshot(self) -> dict:
        return {"seen": list(self.state["seen"])}

    def restore(self, snap: dict) -> None:
        self.state = {"seen": list(snap["seen"])}


class CountingProcessor:
    def __init__(self) -> None:
        self.count = 0

    def accept(self, payload: bytes) -> None:
        self.count += 1

    def results(self):
        return [str(self.count).encode()]


def _state(factory=lambda _cfg: CountingProcessor(), coordinator=None, configuration="cfg"):
    return AggregatorState(configuration, coordinator or FakeCoordinator(), factory)


def test_satisfies_worker_state_protocol():
    assert isinstance(_state(), WorkerState)


def test_apply_change_accumulates_and_counts():
    state = _state()
    for _ in range(3):
        state.apply_change(AggregatorState.data_change(CLIENT, b"x"))
    assert state.data_count(CLIENT) == 3
    assert state._processors_by_client[CLIENT].count == 3


def test_apply_change_isolates_clients():
    state = _state()
    state.apply_change(AggregatorState.data_change(1, b"x"))
    state.apply_change(AggregatorState.data_change(2, b"x"))
    state.apply_change(AggregatorState.data_change(2, b"x"))
    assert state.data_count(1) == 1
    assert state.data_count(2) == 2


def test_data_change_is_json_safe_with_binary_payload():
    import json

    change = AggregatorState.data_change(CLIENT, b"\x00\xff\x01payload")
    assert json.loads(json.dumps(change)) == change


def test_snapshot_restore_round_trip_reproduces_state():
    import pickle

    coordinator = FakeCoordinator()
    coordinator.state["seen"] = [1, 2]
    state = _state(coordinator=coordinator)
    state.apply_change(AggregatorState.data_change(CLIENT, b"x"))
    state.apply_change(AggregatorState.data_change(CLIENT, b"x"))

    snap = pickle.loads(pickle.dumps(state.snapshot()))

    restored = _state(coordinator=FakeCoordinator())
    restored.restore(snap)
    assert restored.data_count(CLIENT) == 2
    assert restored._processors_by_client[CLIENT].count == 2
    assert restored._coordinator.state == {"seen": [1, 2]}


def test_replay_matches_live_for_real_q5_processor():
    live = _state(factory=create_aggregator_processor, configuration=C_Q5)
    for _ in range(5):
        live.apply_change(AggregatorState.data_change(CLIENT, b"tx"))

    replay = _state(factory=create_aggregator_processor, configuration=C_Q5)
    for _ in range(5):
        replay.apply_change(AggregatorState.data_change(CLIENT, b"tx"))

    live_proc = live._processors_by_client[CLIENT]
    replay_proc = replay._processors_by_client[CLIENT]
    assert live_proc.results() == replay_proc.results()


def test_restore_empty_is_fresh_start():
    state = _state()
    state.apply_change(AggregatorState.data_change(CLIENT, b"x"))
    state.restore({})
    assert state.data_count(CLIENT) == 0


def test_closed_client_change_is_ignored():
    state = _state()
    state._closed_by_client.add(CLIENT)
    state.apply_change(AggregatorState.data_change(CLIENT, b"x"))
    assert state.data_count(CLIENT) == 0


def test_close_change_drops_client_and_marks_closed():
    state = _state()
    state.apply_change(AggregatorState.data_change(CLIENT, b"x"))
    state.apply_change(AggregatorState.close_change(CLIENT))
    assert CLIENT in state._closed_by_client
    assert CLIENT not in state._processors_by_client
    assert state.data_count(CLIENT) == 0


def test_data_after_close_change_is_ignored():
    state = _state()
    state.apply_change(AggregatorState.close_change(CLIENT))
    state.apply_change(AggregatorState.data_change(CLIENT, b"x"))
    assert state.data_count(CLIENT) == 0


def test_close_change_is_idempotent_on_replay():
    state = _state()
    state.apply_change(AggregatorState.close_change(CLIENT))
    state.apply_change(AggregatorState.close_change(CLIENT))
    assert CLIENT in state._closed_by_client


def test_results_for_reads_before_close():
    state = _state()
    for _ in range(3):
        state.apply_change(AggregatorState.data_change(CLIENT, b"x"))
    assert state.results_for(CLIENT) == [b"3"]
    state.apply_change(AggregatorState.close_change(CLIENT))
    assert state.results_for(CLIENT) == []


def test_unknown_change_type_raises():
    import pytest

    with pytest.raises(ValueError):
        _state().apply_change({"type": "bogus", "client_id": CLIENT})


# --- EOF flows through the real handler exactly like any other message ---


def _new_handler(tmp_path, state):
    return PersistentStateHandler(
        state_dir=str(tmp_path), node_id="agg", worker_state=state, snapshot_every=1000
    )


def _eof_business(state):
    def fn(_payload):
        outputs = [
            OutboxEntry(f"eof:{CLIENT}#{i}", "eof", "dest", body)
            for i, body in enumerate(state.results_for(CLIENT) + [b"EOF"])
        ]
        return AggregatorState.close_change(CLIENT), outputs

    return fn


def _feed_data(handler, seq, n=1):
    for _ in range(n):
        instr = handler.handle(
            f"d{seq}", CLIENT, 1, seq, b"x",
            lambda _p: (AggregatorState.data_change(CLIENT, b"x"), []),
        )
        handler.commit_done(*instr.ctx)


def test_eof_input_emits_results_and_dedups_redelivery(tmp_path):
    state = _state()
    handler = _new_handler(tmp_path, state)
    for seq in (1, 2, 3):
        _feed_data(handler, seq)

    instr = handler.handle("eof", CLIENT, 1, 4, b"", _eof_business(state))
    assert instr.action is Action.PUBLISH_THEN_COMMIT
    assert [e.output_id for e in instr.outputs] == [f"eof:{CLIENT}#0", f"eof:{CLIENT}#1"]
    handler.commit_done(*instr.ctx)
    assert CLIENT in state._closed_by_client

    redelivered = handler.handle("eof", CLIENT, 1, 4, b"", _eof_business(state))
    assert redelivered.action is Action.ACK


def test_eof_outputs_republished_after_crash_before_commit(tmp_path):
    state = _state()
    handler = _new_handler(tmp_path, state)
    _feed_data(handler, 1)
    handler.handle("eof", CLIENT, 1, 4, b"", _eof_business(state))  # applied, not committed
    handler.wal.close()

    recovered_state = _state()
    recovered = _new_handler(tmp_path, recovered_state)
    recovered.recover()
    assert CLIENT in recovered_state._closed_by_client
    assert [e.output_id for e in recovered.outbox_to_republish()] == [
        f"eof:{CLIENT}#0",
        f"eof:{CLIENT}#1",
    ]
