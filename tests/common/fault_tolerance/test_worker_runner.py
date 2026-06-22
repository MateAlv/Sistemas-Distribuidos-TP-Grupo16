"""WorkerRunner drives the handler for the data plane: publish-with-confirms then
commit then ack; redelivery of a committed input is a no-op ack; a business error
nacks with requeue=True; recovery re-publishes the pending outbox."""

import threading

from common.fault_tolerance.handler.persistent_state_handler import (
    PersistentStateHandler,
)
from common.fault_tolerance.handler.worker_runner import WorkerRunner
from common.fault_tolerance.inbox import InboxStatus
from common.message_protocol.internal.protocol import InternalProtocol
from common.message_protocol.internal.common.message_type import MessageType

from tests.common.fault_tolerance.fakes import FakeWorkerState, change

CLIENT = 7
SENDER = 1


class FakePublisher:
    def __init__(self):
        self.bodies = []

    def send(self, body):
        self.bodies.append(body)


class Calls:
    def __init__(self):
        self.acks = 0
        self.nacks = []

    def ack(self):
        self.acks += 1

    def nack(self, requeue=False):
        self.nacks.append(requeue)


def _handler(state_dir, worker_state=None):
    return PersistentStateHandler(
        state_dir=str(state_dir),
        node_id="node_a",
        worker_state=worker_state or FakeWorkerState(),
        snapshot_every=1000,
    )


def _packet(delta: int, seq: int) -> bytes:
    return InternalProtocol.create_addressed_packet(
        MessageType.DATA,
        CLIENT.to_bytes(16, byteorder="big"),
        SENDER,
        seq,
        delta.to_bytes(4, byteorder="big"),
    )


def _payload_fn(_client_id, payload):
    delta = int.from_bytes(payload, byteorder="big")
    return change(delta), [("dest", b"out-" + payload)]


def test_process_new_publishes_then_commits_then_acks(tmp_path):
    handler = _handler(tmp_path)
    pub = FakePublisher()
    runner = WorkerRunner(handler, {"dest": pub}, _payload_fn, threading.Lock())
    calls = Calls()

    runner.process(_packet(5, 0), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == []
    assert pub.bodies == [b"out-" + (5).to_bytes(4, "big")]
    assert handler.worker_state.total == 5
    assert handler.inbox.classify(CLIENT, SENDER, 0) is InboxStatus.DONE


def test_redelivery_of_committed_input_is_acked_not_republished(tmp_path):
    handler = _handler(tmp_path)
    pub = FakePublisher()
    runner = WorkerRunner(handler, {"dest": pub}, _payload_fn, threading.Lock())
    calls = Calls()

    packet = _packet(5, 0)
    runner.process(packet, calls.ack, calls.nack)
    runner.process(packet, calls.ack, calls.nack)  # redelivery

    assert calls.acks == 2
    assert len(pub.bodies) == 1  # not republished
    assert handler.worker_state.total == 5  # not double counted


def test_business_error_nacks_with_requeue(tmp_path):
    handler = _handler(tmp_path)

    def boom(_client_id, _payload):
        raise ValueError("bad payload")

    runner = WorkerRunner(handler, {}, boom, threading.Lock())
    calls = Calls()

    runner.process(_packet(5, 0), calls.ack, calls.nack)

    assert calls.acks == 0
    assert calls.nacks == [True]


def test_recover_republishes_pending_outbox(tmp_path):
    # Apply an input but never commit it (a crash between publish and commit), so
    # its outputs are left pending in the outbox.
    crashed = _handler(tmp_path)
    crashed.handle(
        "1:0", CLIENT, SENDER, 0, (5).to_bytes(4, "big"),
        lambda data: _payload_fn(CLIENT, data),
    )
    crashed.wal.close()

    pub = FakePublisher()
    runner = WorkerRunner(_handler(tmp_path), {"dest": pub}, _payload_fn, threading.Lock())
    runner.recover_and_republish()

    assert pub.bodies == [b"out-" + (5).to_bytes(4, "big")]
