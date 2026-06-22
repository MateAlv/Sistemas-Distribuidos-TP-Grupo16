from common.constants import C_Q5
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import (
    PersistentStateHandler,
)
from common.message_protocol.internal.aggregation_serializer import AggregationSerializer
from workers.joiner.joiner_state import JoinerState
from workers.joiner.processors import create_joiner_processor

CLIENT = 7


def _state():
    return JoinerState(lambda: create_joiner_processor(C_Q5))


def _handler(tmp_path, state):
    return PersistentStateHandler(
        state_dir=str(tmp_path),
        node_id="joiner",
        worker_state=state,
        snapshot_every=1000,
    )


def _feed_data(handler, seq: int, value: int):
    payload = AggregationSerializer.serialize(value)
    instr = handler.handle(
        f"d{seq}",
        CLIENT,
        1,
        seq,
        payload,
        lambda pl: (JoinerState.data_change(CLIENT, pl), []),
    )
    handler.commit_done(*instr.ctx)


def _eof_business(state):
    def fn(_payload):
        outputs = [
            ("dest", body)
            for body in state.results_for(CLIENT) + [b"EOF"]
        ]
        return JoinerState.close_change(CLIENT), outputs

    return fn


def _oid(seq: int, index: int) -> str:
    return f"joiner:{CLIENT}:{seq}#{index}"


def test_final_eof_outputs_republished_after_crash_before_commit(tmp_path):
    state = _state()
    handler = _handler(tmp_path, state)
    _feed_data(handler, 0, 3)
    _feed_data(handler, 1, 7)

    instr = handler.handle("eof", CLIENT, 1, 2, b"", _eof_business(state))
    assert instr.action is Action.PUBLISH_THEN_COMMIT
    assert len(instr.outputs) == 2
    assert AggregationSerializer.deserialize(instr.outputs[0].body) == 10
    handler.wal.close()

    recovered_state = _state()
    recovered = _handler(tmp_path, recovered_state)
    recovered.recover()

    assert recovered_state.is_closed(CLIENT)
    assert [e.output_id for e in recovered.outbox_to_republish()] == [
        _oid(0, 0),
        _oid(1, 1),
    ]
