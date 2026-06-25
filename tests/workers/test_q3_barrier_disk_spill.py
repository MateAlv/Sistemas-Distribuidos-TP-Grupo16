import importlib
import sys
import types

from common.domain.partial_result import Q3AverageResult
from common.domain.transaction import Transaction
from common.message_protocol.internal import (
    ControlMessage,
    ControlMessageSerializer,
    InternalProtocol,
    Q3AverageResultSerializer,
    TransactionSerializer,
)
from common.message_protocol.internal.common import MessageType
from workers.q3_barrier.q3_barrier_state import _DiskLog


class FakeQueue:
    def __init__(self, *args, **kwargs):
        self.sent = []
        self.closed = False

    def send(self, message):
        self.sent.append(message)

    def close(self):
        self.closed = True

    def start_consuming(self, callback):
        pass

    def stop_consuming(self):
        pass

    def request_stop_consuming(self):
        pass


class AckNack:
    def __init__(self):
        self.acks = 0
        self.nacks = []

    def ack(self):
        self.acks += 1

    def nack(self, requeue=False):
        self.nacks.append(requeue)


def _import_barrier_module(monkeypatch, tmp_path):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("Q3_AVERAGES_QUEUE", "q3_averages_0")
    monkeypatch.setenv("Q3_CANDIDATES_QUEUE", "q3_candidates_0")
    monkeypatch.setenv("GATEWAY_Q3_QUEUE", "gateway_q3_results_queue")
    monkeypatch.setenv("Q3_THRESHOLD_DIVISOR", "100")
    monkeypatch.setenv("Q3_BARRIER_AMOUNT", "1")
    monkeypatch.setenv("Q3_AVERAGES_EXCHANGE", "q3_averages_exchange")
    monkeypatch.setenv("Q3_CANDIDATES_EXCHANGE", "q3_candidates_exchange")
    monkeypatch.setenv("Q3_AVERAGES_ROUTING_PREFIX", "q3_averages")
    monkeypatch.setenv("Q3_CANDIDATES_ROUTING_PREFIX", "q3_candidates")
    monkeypatch.setenv("STATE_DIR", str(tmp_path))

    fake_pika = types.SimpleNamespace(
        exceptions=types.SimpleNamespace(
            AMQPConnectionError=Exception,
            AMQPChannelError=Exception,
            StreamLostError=Exception,
        ),
        BasicProperties=lambda *args, **kwargs: None,
        BlockingConnection=lambda *args, **kwargs: None,
        ConnectionParameters=lambda *args, **kwargs: None,
    )
    monkeypatch.setitem(sys.modules, "pika", fake_pika)
    sys.modules.pop("workers.q3_barrier.q3_barrier", None)
    module = importlib.import_module("workers.q3_barrier.q3_barrier")
    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", FakeQueue)
    monkeypatch.setattr(module.middleware, "MessageMiddlewareExchangeRabbitMQ", FakeQueue)
    return module


def _tx(amount: float, fmt: str = "Wire") -> Transaction:
    return Transaction(
        date="2022/09/07 00:08",
        from_bank="1",
        from_account="abc",
        to_bank="2",
        to_account="def",
        amount=amount,
        currency="US Dollar",
        format=fmt,
    )


def _pack(proto, msg_type, client_id, payload, seq=0, sender_id=0) -> bytes:
    return proto.create_addressed_packet(
        msg_type=msg_type,
        client_id_bytes=client_id.to_bytes(16, "big"),
        sender_id=sender_id,
        seq=seq,
        payload=payload,
    )


def _handle_average(worker, message, output=None):
    calls = AckNack()
    output = output or FakeQueue()
    worker._handle_average(message, calls.ack, calls.nack, output)
    assert calls.nacks == []
    assert calls.acks == 1
    return output


def _handle_candidate(worker, message, output=None):
    calls = AckNack()
    output = output or FakeQueue()
    worker._handle_candidate(message, calls.ack, calls.nack, output)
    assert calls.nacks == []
    assert calls.acks == 1
    return output


def test_disk_log_roundtrip_preserves_bytes_in_order(tmp_path):
    log = _DiskLog(str(tmp_path / "candidates.log"))
    try:
        payloads = [b"\x00\x01\x02", b"hello world", b"x" * 1024, b""]
        for payload in payloads:
            log.append(payload)

        assert list(log.iter_raw_batches()) == payloads
        assert log.byte_count == sum(4 + len(payload) for payload in payloads)
    finally:
        log.delete()


def test_disk_log_iter_is_a_generator_not_a_list(tmp_path):
    log = _DiskLog(str(tmp_path / "candidates.log"))
    try:
        log.append(b"a")
        log.append(b"b")

        import types as _types

        assert isinstance(log.iter_raw_batches(), _types.GeneratorType)
    finally:
        log.delete()


def test_disk_log_close_releases_file(tmp_path):
    log = _DiskLog(str(tmp_path / "candidates.log"))
    log.append(b"data")
    file_obj = log._file
    log.close()
    assert file_obj.closed


def test_candidate_message_stored_raw_without_deserialization(monkeypatch, tmp_path):
    module = _import_barrier_module(monkeypatch, tmp_path)
    worker = module.Q3BarrierWorker()
    proto = InternalProtocol()

    raw_batch = TransactionSerializer.serialize_batch(
        [_tx(amount=10.0, fmt="Wire"), _tx(amount=20.0, fmt="ACH")]
    )
    _handle_candidate(
        worker,
        _pack(proto, MessageType.DATA, client_id=42, payload=raw_batch, seq=0),
    )

    disk_log = worker._state.disk_log(42)
    assert disk_log is not None
    assert list(disk_log.iter_raw_batches()) == [raw_batch]

    worker._handler.wal.close()
    worker.close()


def test_emit_client_filters_using_disk_log(monkeypatch, tmp_path):
    module = _import_barrier_module(monkeypatch, tmp_path)
    worker = module.Q3BarrierWorker()
    proto = InternalProtocol()
    output = FakeQueue()
    client_id = 7

    for seq, (fmt, avg) in enumerate([("Wire", 1000.0), ("ACH", 200.0)]):
        avg_payload = Q3AverageResultSerializer.serialize(
            Q3AverageResult(payment_format=fmt, average=avg)
        )
        _handle_average(
            worker,
            _pack(proto, MessageType.DATA, client_id, avg_payload, seq=seq),
            output,
        )

    candidates = [
        _tx(amount=5.0, fmt="Wire"),
        _tx(amount=15.0, fmt="Wire"),
        _tx(amount=0.5, fmt="ACH"),
    ]
    raw_batch = TransactionSerializer.serialize_batch(candidates)
    _handle_candidate(
        worker,
        _pack(proto, MessageType.DATA, client_id, raw_batch, seq=0),
        output,
    )

    eof_ctrl = ControlMessageSerializer.serialize(
        ControlMessage(sender_id=0, expected_total=3, processed_count=0)
    )
    _handle_average(
        worker,
        _pack(proto, MessageType.EOF, client_id, eof_ctrl, seq=2),
        output,
    )
    _handle_candidate(
        worker,
        _pack(proto, MessageType.EOF, client_id, eof_ctrl, seq=1),
        output,
    )

    assert len(output.sent) == 2
    data_msg, eof_msg = output.sent

    data_type, data_cid, data_payload = proto.unpack_packet(data_msg)
    assert data_type == MessageType.DATA
    assert data_cid == client_id
    emitted_txs = TransactionSerializer.deserialize_batch(data_payload)
    assert sorted(t.amount for t in emitted_txs) == [0.5, 5.0]

    eof_type, eof_cid, eof_payload = proto.unpack_packet(eof_msg)
    assert eof_type == MessageType.EOF
    assert eof_cid == client_id
    eof_ctrl_msg = ControlMessageSerializer.deserialize(eof_payload)
    assert eof_ctrl_msg.expected_total == 2

    worker._handler.wal.close()
    worker.close()


def test_emit_closes_disk_log(monkeypatch, tmp_path):
    module = _import_barrier_module(monkeypatch, tmp_path)
    worker = module.Q3BarrierWorker()
    proto = InternalProtocol()
    client_id = 1

    raw_batch = TransactionSerializer.serialize_batch([_tx(amount=1.0, fmt="Wire")])
    _handle_candidate(
        worker,
        _pack(proto, MessageType.DATA, client_id, raw_batch, seq=0),
    )
    file_obj = worker._state.disk_log(client_id)._file

    eof_ctrl = ControlMessageSerializer.serialize(
        ControlMessage(sender_id=0, expected_total=1, processed_count=0)
    )
    _handle_average(
        worker,
        _pack(proto, MessageType.EOF, client_id, eof_ctrl, seq=0),
    )
    _handle_candidate(
        worker,
        _pack(proto, MessageType.EOF, client_id, eof_ctrl, seq=1),
    )

    assert file_obj.closed
    assert worker._state.disk_log(client_id) is None

    worker._handler.wal.close()
    worker.close()


def test_crash_before_first_snapshot_does_not_double_candidate_log(monkeypatch, tmp_path):
    """Regression: a crash before the first snapshot must not double the candidate
    disk log. With no snapshot, recover() skips restore() and REPLAY_ALL re-appends
    every candidate; start() must drop the stale file first or each candidate emits
    twice (the Q3 client doubling seen under chaos)."""
    module = _import_barrier_module(monkeypatch, tmp_path)
    proto = InternalProtocol()
    client_id = 5

    raw_batches = [
        TransactionSerializer.serialize_batch([_tx(amount=1.0, fmt="Wire")]),
        TransactionSerializer.serialize_batch([_tx(amount=2.0, fmt="ACH")]),
    ]

    worker = module.Q3BarrierWorker()
    for seq, raw in enumerate(raw_batches):
        _handle_candidate(
            worker, _pack(proto, MessageType.DATA, client_id, raw, seq=seq)
        )
    assert list(worker._state.disk_log(client_id).iter_raw_batches()) == raw_batches
    # Crash: no snapshot was taken; the disk log + WAL persist under STATE_DIR.
    worker._handler.wal.close()
    worker._state.disk_log(client_id).close()

    # Restart on the same STATE_DIR: start() runs recovery and must rebuild once.
    recovered = module.Q3BarrierWorker()
    assert recovered._handler.last_state.load() is None
    recovered.start()

    assert (
        list(recovered._state.disk_log(client_id).iter_raw_batches()) == raw_batches
    )

    recovered._handler.wal.close()
    recovered.close()


def test_abort_closes_pending_client_state(monkeypatch, tmp_path):
    module = _import_barrier_module(monkeypatch, tmp_path)
    worker = module.Q3BarrierWorker()
    proto = InternalProtocol()
    client_id = 100

    raw_batch = TransactionSerializer.serialize_batch([_tx(amount=1.0)])
    _handle_candidate(
        worker,
        _pack(proto, MessageType.DATA, client_id, raw_batch, seq=0),
    )
    file_obj = worker._state.disk_log(client_id)._file

    _handle_candidate(
        worker,
        _pack(proto, MessageType.ABORT, client_id, b"", seq=1),
    )

    assert file_obj.closed
    assert worker._state.disk_log(client_id) is None
    assert worker._state.is_closed(client_id)

    worker._handler.wal.close()
    worker.close()
