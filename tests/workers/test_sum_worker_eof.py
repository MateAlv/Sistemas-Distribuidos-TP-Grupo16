"""N=1 upstream-EOF flush for the WAL-wired sum worker.

With a single sum replica `on_upstream_eof` returns FlushAction directly: the
worker emits its partials and the downstream EOF to the aggregator shards and
closes the client. Outputs are addressed packets (sender_id + seq) carried
through the durable outbox; these tests capture them via a recording exchange.
"""

import importlib
import sys
import types

from common.message_protocol.internal.common import ControlMessage, MessageType


class FakeQueue:
    def __init__(self, *args, **kwargs):
        self.sent = []

    def send(self, message, *args, **kwargs):
        self.sent.append(message)

    def close(self):
        pass

    def start_consuming(self, callback):
        pass

    def stop_consuming(self):
        pass


def _noop_ack():
    pass


def _noop_nack(requeue=False):
    pass


def _recording_exchange(registry):
    """Exchange stand-in that records each send under its routing key. The worker
    builds it as MessageMiddlewareExchangeRabbitMQ(host, exchange, [routing_key])."""

    class RecordingExchange:
        def __init__(self, *args, **kwargs):
            routing_keys = args[2] if len(args) >= 3 else kwargs.get("routing_keys", [])
            self.routing_key = routing_keys[0] if routing_keys else None

        def send(self, message, *args, **kwargs):
            registry.setdefault(self.routing_key, []).append(message)

        def close(self):
            pass

    return RecordingExchange


def _import_sum_module(monkeypatch, tmp_path, registry):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("INPUT_QUEUE", "sum_0")
    monkeypatch.setenv("INPUT_EXCHANGE", "sum_exchange")
    monkeypatch.setenv("INPUT_ROUTING_PREFIX", "sum")
    monkeypatch.setenv("CONFIGURATION", "Q2")
    monkeypatch.setenv("SUM_AMOUNT", "1")
    monkeypatch.setenv("SUM_PREFIX", "sum")
    monkeypatch.setenv("AGGREGATION_AMOUNT", "1")
    monkeypatch.setenv("AGGREGATION_PREFIX", "aggregation")
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
    sys.modules.pop("workers.sum.sums", None)
    module = importlib.import_module("workers.sum.sums")
    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", FakeQueue)
    monkeypatch.setattr(
        module.middleware, "MessageMiddlewareExchangeRabbitMQ", _recording_exchange(registry)
    )
    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", FakeQueue)
    monkeypatch.setattr(module, "LazyQueue", FakeQueue)
    return module


def _addressed_eof(worker, client_id: int, seq: int, expected_total: int) -> bytes:
    payload = worker._control_serializer.serialize(
        ControlMessage(sender_id=0, expected_total=expected_total, processed_count=0)
    )
    return worker._internal_protocol.create_addressed_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=0,
        seq=seq,
        payload=payload,
    )


def test_single_worker_eof_flushes_partials_and_closes(monkeypatch, tmp_path):
    registry: dict = {}
    module = _import_sum_module(monkeypatch, tmp_path, registry)
    worker = module.SumWorker()
    client_id = 7

    monkeypatch.setattr(
        worker._state, "partials_for", lambda _cid: [("001", b"serialized-partial")]
    )

    worker._process_message(
        _addressed_eof(worker, client_id, seq=0, expected_total=3), _noop_ack, _noop_nack
    )

    sent = registry["aggregation_0"]
    assert len(sent) == 2

    data_type, data_client_id, data_sender, data_seq, data_payload = (
        worker._internal_protocol.unpack_addressed_packet(sent[0])
    )
    eof_type, eof_client_id, eof_sender, eof_seq, eof_payload = (
        worker._internal_protocol.unpack_addressed_packet(sent[1])
    )

    assert data_type == MessageType.DATA
    assert data_client_id == client_id
    assert data_sender == 0
    assert data_seq == 0
    assert data_payload == b"serialized-partial"

    assert eof_type == MessageType.EOF
    assert eof_seq == 1
    assert worker._control_serializer.deserialize(eof_payload).expected_total == 1

    # The flush closed the client (close_change applied through the WAL).
    assert worker._state.is_closed(client_id)


def test_single_worker_eof_without_partials_forwards_zero_expected_total(monkeypatch, tmp_path):
    registry: dict = {}
    module = _import_sum_module(monkeypatch, tmp_path, registry)
    worker = module.SumWorker()
    client_id = 11

    monkeypatch.setattr(worker._state, "partials_for", lambda _cid: [])

    worker._process_message(
        _addressed_eof(worker, client_id, seq=0, expected_total=0), _noop_ack, _noop_nack
    )

    sent = registry["aggregation_0"]
    assert len(sent) == 1
    eof_type, eof_client_id, eof_sender, eof_seq, eof_payload = (
        worker._internal_protocol.unpack_addressed_packet(sent[0])
    )
    assert eof_type == MessageType.EOF
    assert eof_client_id == client_id
    assert eof_sender == 0
    assert eof_seq == 0
    assert worker._control_serializer.deserialize(eof_payload).expected_total == 0
    assert worker._state.is_closed(client_id)
