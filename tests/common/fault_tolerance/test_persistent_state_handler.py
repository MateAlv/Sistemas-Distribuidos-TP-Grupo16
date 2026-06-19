import pytest

from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import (
    PersistentStateHandler,
)
from common.fault_tolerance.inbox import InboxStatus

from tests.common.fault_tolerance.fakes import (
    CrashError,
    FakeLastState,
    FakeWal,
    FakeWorkerState,
    change,
)

CLIENT = 7
SENDER = 1


def _handler(wal, last_state, worker_state, snapshot_every=1000):
    return PersistentStateHandler(
        state_dir="unused",
        node_id="node_a",
        worker_state=worker_state,
        snapshot_every=snapshot_every,
        last_state=last_state,
        wal=wal,
    )


def _oid(seq: int, index: int = 0, client: int = CLIENT, node: str = "node_a") -> str:
    return f"{node}:{client}:{seq}#{index}"


def _business(value: int, n_outputs: int = 1):
    def fn(_payload: bytes):
        outputs = [("dest", b"body") for _ in range(n_outputs)]
        return change(value), outputs
    return fn


def _process_fully(handler, msg_id, seq, value, n_outputs=1):
    instruction = handler.handle(
        msg_id, CLIENT, SENDER, seq, b"payload", _business(value, n_outputs)
    )
    if instruction.action is Action.PUBLISH_THEN_COMMIT:
        handler.commit_done(*instruction.ctx)
    return instruction


# ─── live behaviour ──────────────────────────────────────────────────────────

def test_new_message_asks_to_publish_then_commit():
    handler = _handler(FakeWal(), FakeLastState(), FakeWorkerState())
    instruction = handler.handle(
        "m1", CLIENT, SENDER, 1, b"payload", _business(10)
    )
    assert instruction.action is Action.PUBLISH_THEN_COMMIT
    assert [e.output_id for e in instruction.outputs] == [_oid(0)]
    assert handler.worker_state.total == 10


def test_duplicate_done_is_acked_without_reprocessing():
    worker = FakeWorkerState()
    handler = _handler(FakeWal(), FakeLastState(), worker)
    _process_fully(handler, "m1", 1, 10)

    instruction = handler.handle(
        "m1", CLIENT, SENDER, 1, b"payload", _business(10)
    )
    assert instruction.action is Action.ACK
    assert worker.total == 10  # not double counted


def test_redelivered_applied_skips_business_and_republishes():
    worker = FakeWorkerState()
    handler = _handler(FakeWal(), FakeLastState(), worker)
    # applied but not committed (no commit_done)
    handler.handle("m1", CLIENT, SENDER, 1, b"payload", _business(10))

    def must_not_run(_payload):
        raise AssertionError("business_fn must not run for an APPLIED redelivery")

    instruction = handler.handle("m1", CLIENT, SENDER, 1, b"payload", must_not_run)
    assert instruction.action is Action.PUBLISH_THEN_COMMIT
    assert [e.output_id for e in instruction.outputs] == [_oid(0)]
    assert worker.total == 10


# ─── crash recovery (spec "Acción al recuperar" table) ───────────────────────

def test_crash_before_input_applied_persisted_reprocesses_as_new():
    wal, last_state = FakeWal(), FakeLastState()
    wal.raise_on_append = 1
    handler = _handler(wal, last_state, FakeWorkerState())

    with pytest.raises(CrashError):
        handler.handle("m1", CLIENT, SENDER, 1, b"payload", _business(10))

    recovered = _handler(wal, last_state, FakeWorkerState())
    recovered.recover()
    assert recovered.inbox.classify(CLIENT, SENDER, 1) is InboxStatus.NEW
    assert recovered.worker_state.total == 0


def test_crash_after_applied_before_done_republishes_and_keeps_state_once():
    wal, last_state = FakeWal(), FakeLastState()
    handler = _handler(wal, last_state, FakeWorkerState())
    handler.handle("m1", CLIENT, SENDER, 1, b"payload", _business(10))
    # crash before commit_done

    recovered = _handler(wal, last_state, FakeWorkerState())
    recovered.recover()
    assert recovered.inbox.classify(CLIENT, SENDER, 1) is InboxStatus.APPLIED
    assert recovered.worker_state.total == 10
    assert [e.output_id for e in recovered.outbox_to_republish()] == [_oid(0)]


def test_crash_after_done_before_ack_is_done_with_empty_outbox():
    wal, last_state = FakeWal(), FakeLastState()
    handler = _handler(wal, last_state, FakeWorkerState())
    _process_fully(handler, "m1", 1, 10)
    # crash before RabbitMQ ack

    recovered = _handler(wal, last_state, FakeWorkerState())
    recovered.recover()
    assert recovered.inbox.classify(CLIENT, SENDER, 1) is InboxStatus.DONE
    assert recovered.worker_state.total == 10
    assert recovered.outbox_to_republish() == []


def test_recover_with_no_snapshot_replays_whole_wal():
    wal, last_state = FakeWal(), FakeLastState()
    handler = _handler(wal, last_state, FakeWorkerState())
    _process_fully(handler, "m1", 1, 10)
    _process_fully(handler, "m2", 2, 20)

    recovered = _handler(wal, last_state, FakeWorkerState())
    recovered.recover()
    assert recovered.worker_state.total == 30
    assert recovered.inbox.classify(CLIENT, SENDER, 2) is InboxStatus.DONE


# ─── snapshot windows ────────────────────────────────────────────────────────

def test_crash_after_commit_before_rotate_does_not_double_apply():
    wal, last_state = FakeWal(), FakeLastState()
    wal.raise_on_rotate = True
    handler = _handler(wal, last_state, FakeWorkerState(), snapshot_every=2)

    _process_fully(handler, "m1", 1, 10)
    with pytest.raises(CrashError):
        _process_fully(handler, "m2", 2, 20)  # triggers snapshot -> rotate

    # snapshot committed (total 30) but WAL still holds all 4 records un-rotated
    recovered = _handler(wal, last_state, FakeWorkerState(), snapshot_every=2)
    recovered.recover()
    assert recovered.worker_state.total == 30  # not 60
    assert recovered.inbox.classify(CLIENT, SENDER, 1) is InboxStatus.DONE
    assert recovered.inbox.classify(CLIENT, SENDER, 2) is InboxStatus.DONE


def test_recover_from_snapshot_plus_fresh_wal():
    wal, last_state = FakeWal(), FakeLastState()
    handler = _handler(wal, last_state, FakeWorkerState(), snapshot_every=2)
    _process_fully(handler, "m1", 1, 10)
    _process_fully(handler, "m2", 2, 20)  # snapshot + rotate ok
    assert wal.records == []  # rotated
    # one more, applied but not done -> only in the fresh WAL
    handler.handle("m3", CLIENT, SENDER, 3, b"payload", _business(5))

    recovered = _handler(wal, last_state, FakeWorkerState(), snapshot_every=2)
    recovered.recover()
    assert recovered.worker_state.total == 35
    assert recovered.inbox.classify(CLIENT, SENDER, 3) is InboxStatus.APPLIED


def test_snapshot_resets_counter_and_rotates():
    wal, last_state = FakeWal(), FakeLastState()
    handler = _handler(wal, last_state, FakeWorkerState(), snapshot_every=2)
    _process_fully(handler, "m1", 1, 10)
    _process_fully(handler, "m2", 2, 20)
    assert handler.applied_since_snapshot == 0
    assert last_state.load() is not None
