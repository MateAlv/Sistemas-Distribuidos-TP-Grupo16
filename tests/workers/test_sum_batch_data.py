import importlib
import sys
import types

from common.domain.transaction import Transaction
from common.message_protocol.internal import (
    InternalProtocol,
    TransactionSerializer,
)
from common.message_protocol.internal.common import MessageType


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


def _import_sum_module(monkeypatch):
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


def _tx(amount: float, currency: str = "US Dollar") -> Transaction:
    return Transaction(
        date="2022/09/01 00:08",
        from_bank="bank_1",
        from_account="acc_a",
        to_bank="bank_2",
        to_account="acc_b",
        amount=amount,
        currency=currency,
        format="Wire",
    )


def _data_packet(client_id: int, transactions: list[Transaction]) -> bytes:
    return InternalProtocol.create_packet(
        msg_type=MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=TransactionSerializer.serialize_batch(transactions),
    )


def test_sum_processed_count_scales_with_batch_size(monkeypatch):
    # SumWorker debe contar las N transactions del batch, no 1 por DATA.
    module = _import_sum_module(monkeypatch)
    worker = module.SumWorker()
    client_id = 5

    transactions = [_tx(10.0), _tx(20.0), _tx(30.0)]
    msg_type, client_id_unpacked, payload = InternalProtocol.unpack_packet(
        _data_packet(client_id, transactions)
    )

    worker._handle_data_packet(client_id_unpacked, payload)

    assert worker._processed_by_client[client_id] == 3


def test_sum_single_transaction_payload_still_counts_one(monkeypatch):
    # Backwards-compat: payload con 1 tx (formato pre-batching).
    module = _import_sum_module(monkeypatch)
    worker = module.SumWorker()

    msg_type, client_id, payload = InternalProtocol.unpack_packet(
        _data_packet(9, [_tx(50.0)])
    )
    worker._handle_data_packet(client_id, payload)

    assert worker._processed_by_client[9] == 1
