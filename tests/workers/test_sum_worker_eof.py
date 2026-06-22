import importlib
import sys
import types

from common.message_protocol.internal.common import ControlMessage, MessageType


class FakeQueue:
    def __init__(self, *args, **kwargs):
        self.sent = []

    def send(self, message):
        self.sent.append(message)

    def close(self):
        pass

    def start_consuming(self, callback):
        pass

    def stop_consuming(self):
        pass


class FakeExchange(FakeQueue):
    pass


def import_sum_module(monkeypatch):
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
        module.middleware,
        "MessageMiddlewareExchangeRabbitMQ",
        FakeExchange,
    )
    return module


def test_single_worker_eof_snapshots_partials_while_holding_lock(monkeypatch):
    module = import_sum_module(monkeypatch)
    worker = module.SumWorker()
    client_id = 7
    output_exchange = FakeExchange()
    worker._output_exchanges = [output_exchange]
    worker._processed_by_client[client_id] = 3

    def partials_for_client(client_id_received):
        assert client_id_received == client_id
        assert worker._lock.locked()
        return [("001", b"serialized-partial")]

    monkeypatch.setattr(worker, "_partials_for_client", partials_for_client)

    payload = worker._control_serializer.serialize(
        ControlMessage(sender_id=0, expected_total=3, processed_count=0)
    )

    worker._handle_upstream_eof(client_id, payload)

    assert client_id not in worker._processed_by_client
    assert len(output_exchange.sent) == 2

    data_type, data_client_id, data_sender, data_seq, data_payload = (
        worker._internal_protocol.unpack_addressed_packet(output_exchange.sent[0])
    )
    eof_type, eof_client_id, eof_sender, eof_seq, eof_payload = (
        worker._internal_protocol.unpack_addressed_packet(output_exchange.sent[1])
    )

    assert data_type == MessageType.DATA
    assert data_client_id == client_id
    assert data_sender == 0
    assert data_seq == 0
    assert data_payload == b"serialized-partial"
    assert eof_type == MessageType.EOF
    assert eof_client_id == client_id
    assert eof_sender == 0
    assert eof_seq == 1
    eof_control = worker._control_serializer.deserialize(eof_payload)
    assert eof_control.expected_total == 1


def test_single_worker_eof_without_partials_forwards_zero_expected_total(monkeypatch):
    module = import_sum_module(monkeypatch)
    worker = module.SumWorker()
    client_id = 11
    output_exchange = FakeExchange()
    worker._output_exchanges = [output_exchange]
    worker._processed_by_client[client_id] = 0

    monkeypatch.setattr(worker, "_partials_for_client", lambda _: [])

    payload = worker._control_serializer.serialize(
        ControlMessage(sender_id=0, expected_total=0, processed_count=0)
    )

    worker._handle_upstream_eof(client_id, payload)

    assert len(output_exchange.sent) == 1
    eof_type, eof_client_id, eof_sender, eof_seq, eof_payload = (
        worker._internal_protocol.unpack_addressed_packet(output_exchange.sent[0])
    )
    eof_control = worker._control_serializer.deserialize(eof_payload)
    assert eof_type == MessageType.EOF
    assert eof_client_id == client_id
    assert eof_sender == 0
    assert eof_seq == 0
    assert eof_control.expected_total == 0
