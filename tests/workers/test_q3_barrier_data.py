"""Data-plane unit tests for the WAL-wired q3_barrier.

Covers the barrier semantics (emit the filtered candidates once both streams
EOF), DATA dedup via the durable handler (Action.ACK path), and that the two
streams sharing one inbox never collide thanks to per-stream MsgKind. The disk
log durability round-trip is covered in the Q3BarrierState smoke test.
"""

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


class RecordingSender:
    def __init__(self, *args, **kwargs):
        self.messages = []

    def send(self, message, *args, **kwargs):
        self.messages.append(message)

    def close(self):
        pass

    def start_consuming(self, *_):
        pass

    def stop_consuming(self):
        pass


def _noop_ack():
    pass


def _noop_nack(requeue=False):
    pass


def _tx(amount, fmt="Wire") -> Transaction:
    return Transaction(
        date="2022/09/10 00:08",
        from_bank="bank_1",
        from_account="acc_a",
        to_bank="bank_2",
        to_account="acc_b",
        amount=amount,
        currency="US Dollar",
        format=fmt,
    )


def _addressed(msg_type, client_id, sender_id, seq, payload) -> bytes:
    return InternalProtocol.create_addressed_packet(
        msg_type=msg_type,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=sender_id,
        seq=seq,
        payload=payload,
    )


def _avg_data(client_id, sender_id, seq, payment_format, average) -> bytes:
    return _addressed(
        MessageType.DATA, client_id, sender_id, seq,
        Q3AverageResultSerializer.serialize(
            Q3AverageResult(payment_format=payment_format, average=average)
        ),
    )


def _candidate_data(client_id, sender_id, seq, transactions) -> bytes:
    return _addressed(
        MessageType.DATA, client_id, sender_id, seq,
        TransactionSerializer.serialize_batch(transactions),
    )


def _eof(client_id, sender_id, seq, expected_total=0) -> bytes:
    return _addressed(
        MessageType.EOF, client_id, sender_id, seq,
        ControlMessageSerializer.serialize(
            ControlMessage(sender_id=sender_id, expected_total=expected_total, processed_count=0)
        ),
    )


def _import_worker(monkeypatch, tmp_path):
    env = {
        "ID": "0",
        "MOM_HOST": "rabbitmq",
        "Q3_AVERAGES_QUEUE": "q3_averages_0",
        "Q3_CANDIDATES_QUEUE": "q3_candidates_0",
        "GATEWAY_Q3_QUEUE": "gateway_q3",
        "Q3_AVERAGES_EXCHANGE": "q3_averages_exchange",
        "Q3_CANDIDATES_EXCHANGE": "q3_candidates_exchange",
        "Q3_AVERAGES_ROUTING_PREFIX": "q3_averages",
        "Q3_CANDIDATES_ROUTING_PREFIX": "q3_candidates",
        "STATE_DIR": str(tmp_path),
        "Q3_THRESHOLD_DIVISOR": "100",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    fake_pika = types.SimpleNamespace(
        exceptions=types.SimpleNamespace(
            AMQPConnectionError=Exception,
            AMQPChannelError=Exception,
            StreamLostError=Exception,
        ),
        BasicProperties=lambda *a, **k: None,
        BlockingConnection=lambda *a, **k: None,
        ConnectionParameters=lambda *a, **k: None,
    )
    monkeypatch.setitem(sys.modules, "pika", fake_pika)
    sys.modules.pop("workers.q3_barrier.q3_barrier", None)
    module = importlib.import_module("workers.q3_barrier.q3_barrier")
    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", RecordingSender)
    monkeypatch.setattr(module.middleware, "MessageMiddlewareExchangeRabbitMQ", RecordingSender)
    return module


def _worker(module):
    worker = module.Q3BarrierWorker()
    worker._handler.recover()  # fresh STATE_DIR -> empty
    return worker


def test_barrier_emits_filtered_candidates_on_second_eof(monkeypatch, tmp_path):
    module = _import_worker(monkeypatch, tmp_path)
    worker = _worker(module)
    out = RecordingSender()
    cid = 1

    # Averages stream: Wire -> 500 (threshold 500/100 = 5), then EOF.
    worker._handle_average(_avg_data(cid, 9, 0, "Wire", 500.0), _noop_ack, _noop_nack, out)
    worker._handle_average(_eof(cid, 9, 1), _noop_ack, _noop_nack, out)
    assert out.messages == []  # candidates EOF not here yet -> no emit

    # Candidates stream: one batch (amount 3 passes < 5, amount 10 fails), then EOF.
    worker._handle_candidate(
        _candidate_data(cid, 4, 0, [_tx(3.0), _tx(10.0)]), _noop_ack, _noop_nack, out
    )
    worker._handle_candidate(_eof(cid, 4, 1, expected_total=2), _noop_ack, _noop_nack, out)

    # Second EOF closed the barrier -> emit one DATA batch + EOF.
    assert len(out.messages) == 2
    data_type, _, data_payload = InternalProtocol.unpack_packet(out.messages[0])
    eof_type, _, eof_payload = InternalProtocol.unpack_packet(out.messages[1])
    assert data_type == MessageType.DATA
    result = TransactionSerializer.deserialize_batch(data_payload)
    assert [t.amount for t in result] == [3.0]  # only the below-threshold tx
    assert eof_type == MessageType.EOF
    assert ControlMessageSerializer.deserialize(eof_payload).expected_total == 1
    assert worker._state.is_closed(cid)


def test_duplicate_candidate_is_ignored(monkeypatch, tmp_path):
    module = _import_worker(monkeypatch, tmp_path)
    worker = _worker(module)
    out = RecordingSender()
    cid = 1
    packet = _candidate_data(cid, 4, 0, [_tx(3.0)])

    worker._handle_candidate(packet, _noop_ack, _noop_nack, out)
    worker._handle_candidate(packet, _noop_ack, _noop_nack, out)  # redelivery

    # Only one batch on the disk log despite two deliveries.
    batches = list(worker._state.disk_log(cid).iter_raw_batches())
    assert len(batches) == 1


def test_streams_do_not_collide_on_same_sender_and_seq(monkeypatch, tmp_path):
    # Averages and candidates both arrive as (sender_id=0, seq=0). Separate MsgKinds
    # keep them from colliding in the shared inbox: both must be processed.
    module = _import_worker(monkeypatch, tmp_path)
    worker = _worker(module)
    out = RecordingSender()
    cid = 1

    worker._handle_average(_avg_data(cid, 0, 0, "Wire", 500.0), _noop_ack, _noop_nack, out)
    worker._handle_candidate(_candidate_data(cid, 0, 0, [_tx(3.0)]), _noop_ack, _noop_nack, out)

    assert worker._state.averages(cid) == {"Wire": 500.0}
    assert len(list(worker._state.disk_log(cid).iter_raw_batches())) == 1
