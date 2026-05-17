import importlib
import sys
import types

from common.message_protocol.common import ControlMessage, MessageType


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
    monkeypatch.setenv("INPUT_QUEUE", "sum_input")
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


def test_eof_broadcast_snapshots_partials_while_holding_lock(monkeypatch):
    module = import_sum_module(monkeypatch)
    worker = module.SumWorker()
    client_id = 7
    output_exchange = FakeExchange()
    reports = []
    events = []

    worker.processed_by_client[client_id] = 3

    def partials_for_client(client_id_received):
        assert client_id_received == client_id
        assert worker.lock.locked()
        assert client_id_received not in worker.pending_eof_by_client
        return [("001", b"serialized-partial")]

    def report_to_leader(
        client_id_received,
        leader_id,
        processed_count,
        forwarded_count,
    ):
        reports.append(
            (
                client_id_received,
                leader_id,
                processed_count,
                forwarded_count,
            )
        )

    monkeypatch.setattr(worker, "_partials_for_client", partials_for_client)
    monkeypatch.setattr(worker, "_report_to_leader", report_to_leader)

    payload = worker.control_serializer.serialize(
        ControlMessage(sender_id=0, expected_total=3, processed_count=0)
    )
    message = worker.internal_protocol.create_packet(
        msg_type=MessageType.EOF_RECEIVED,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=payload,
    )

    worker._handle_eof_broadcast(
        message,
        ack=lambda: events.append("ack"),
        nack=lambda: events.append("nack"),
        output_exchanges=[output_exchange],
    )

    assert events == ["ack"]
    assert client_id in worker.pending_eof_by_client
    assert len(output_exchange.sent) == 1
    assert reports == [(client_id, 0, 3, 1)]
