"""End-to-end with the real Wal + real LastState (no fakes), to prove the whole
durable-state stack integrates and recovers."""

from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import (
    PersistentStateHandler,
)
from common.fault_tolerance.inbox import InboxStatus
from common.fault_tolerance.outbox import OutboxEntry

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


def _business(value, *output_ids):
    def fn(_payload):
        outputs = [OutboxEntry(oid, "in", "dest", b"body") for oid in output_ids]
        return change(value), outputs
    return fn


def _process_fully(handler, msg_id, seq, value, *output_ids):
    instruction = handler.handle(
        msg_id, CLIENT, SENDER, seq, b"payload", _business(value, *output_ids)
    )
    if instruction.action is Action.PUBLISH_THEN_COMMIT:
        handler.commit_done(*instruction.ctx)


def test_recover_from_wal_only_no_snapshot(tmp_path):
    handler = _handler(tmp_path, FakeWorkerState())
    _process_fully(handler, "m1", 1, 10, "m1#0")
    _process_fully(handler, "m2", 2, 20, "m2#0")
    handler.wal.close()

    recovered = _handler(tmp_path, FakeWorkerState())
    recovered.recover()
    assert recovered.worker_state.total == 30
    assert recovered.inbox.classify(CLIENT, SENDER, 2) is InboxStatus.DONE
    assert recovered.outbox_to_republish() == []


def test_recover_from_snapshot_plus_fresh_wal(tmp_path):
    handler = _handler(tmp_path, FakeWorkerState(), snapshot_every=2)
    _process_fully(handler, "m1", 1, 10, "m1#0")
    _process_fully(handler, "m2", 2, 20, "m2#0")  # snapshot + rotate
    # third applied but not committed -> only in the fresh WAL
    handler.handle("m3", CLIENT, SENDER, 3, b"payload", _business(5, "m3#0"))
    handler.wal.close()

    recovered = _handler(tmp_path, FakeWorkerState(), snapshot_every=2)
    recovered.recover()
    assert recovered.worker_state.total == 35
    assert recovered.inbox.classify(CLIENT, SENDER, 1) is InboxStatus.DONE
    assert recovered.inbox.classify(CLIENT, SENDER, 3) is InboxStatus.APPLIED
    assert [e.output_id for e in recovered.outbox_to_republish()] == ["m3#0"]


def test_duplicate_after_recovery_is_dropped(tmp_path):
    handler = _handler(tmp_path, FakeWorkerState())
    _process_fully(handler, "m1", 1, 10, "m1#0")
    handler.wal.close()

    recovered = _handler(tmp_path, FakeWorkerState())
    recovered.recover()
    instruction = recovered.handle(
        "m1", CLIENT, SENDER, 1, b"payload", _business(10, "m1#0")
    )
    assert instruction.action is Action.ACK
    assert recovered.worker_state.total == 10  # not double counted
