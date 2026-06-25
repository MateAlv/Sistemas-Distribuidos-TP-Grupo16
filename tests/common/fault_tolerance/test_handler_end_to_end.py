"""End-to-end with the real Wal + real LastState (no fakes), to prove the whole
durable-state stack integrates and recovers."""

from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import (
    PersistentStateHandler,
)
from common.fault_tolerance.inbox import InboxStatus

from tests.common.fault_tolerance.fakes import FakeWorkerState, change

CLIENT = 7
SENDER = 1


def _handler(state_dir, worker_state, snapshot_every=1000):
    return PersistentStateHandler(
        state_dir=str(state_dir),
        node_id="node_a",
        worker_state=worker_state,
        snapshot_every=snapshot_every,
    )


def _oid(seq: int, index: int = 0, client: int = CLIENT, node: str = "node_a") -> str:
    return f"{node}:{client}:{seq}#{index}"


def _business(value, n_outputs=1):
    def fn(_payload):
        outputs = [("dest", b"body") for _ in range(n_outputs)]
        return change(value), outputs
    return fn


def _process_fully(handler, msg_id, seq, value, n_outputs=1):
    instruction = handler.handle(
        msg_id, CLIENT, SENDER, seq, b"payload", _business(value, n_outputs)
    )
    if instruction.action is Action.PUBLISH_THEN_COMMIT:
        handler.commit_done(*instruction.ctx)


def test_recover_from_wal_only_no_snapshot(tmp_path):
    handler = _handler(tmp_path, FakeWorkerState())
    _process_fully(handler, "m1", 1, 10)
    _process_fully(handler, "m2", 2, 20)
    handler.wal.close()

    recovered = _handler(tmp_path, FakeWorkerState())
    recovered.recover()
    assert recovered.worker_state.total == 30
    assert recovered.inbox.classify(CLIENT, SENDER, 2) is InboxStatus.DONE
    assert recovered.outbox_to_republish() == []


def test_recover_from_snapshot_plus_fresh_wal(tmp_path):
    handler = _handler(tmp_path, FakeWorkerState(), snapshot_every=2)
    _process_fully(handler, "m1", 1, 10)
    _process_fully(handler, "m2", 2, 20)  # snapshot + rotate
    # third applied but not committed -> only in the fresh WAL
    handler.handle("m3", CLIENT, SENDER, 3, b"payload", _business(5))
    handler.wal.close()

    recovered = _handler(tmp_path, FakeWorkerState(), snapshot_every=2)
    recovered.recover()
    assert recovered.worker_state.total == 35
    assert recovered.inbox.classify(CLIENT, SENDER, 1) is InboxStatus.DONE
    assert recovered.inbox.classify(CLIENT, SENDER, 3) is InboxStatus.APPLIED
    assert [e.output_id for e in recovered.outbox_to_republish()] == [_oid(2)]


def test_duplicate_after_recovery_is_dropped(tmp_path):
    handler = _handler(tmp_path, FakeWorkerState())
    _process_fully(handler, "m1", 1, 10)
    handler.wal.close()

    recovered = _handler(tmp_path, FakeWorkerState())
    recovered.recover()
    instruction = recovered.handle(
        "m1", CLIENT, SENDER, 1, b"payload", _business(10)
    )
    assert instruction.action is Action.ACK
    assert recovered.worker_state.total == 10  # not double counted
