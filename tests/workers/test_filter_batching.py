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


def _import_filter_module(monkeypatch, configuration: str = "USD"):
    # Solo activamos USD_ENABLE_Q2 para acotar el output del filter al
    # SUM_Q2_QUEUE en este test. El resto se desactiva con flags.
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("CONFIGURATION", configuration)
    monkeypatch.setenv("INPUT_QUEUE", "filter_usd_queue")
    monkeypatch.setenv("GATEWAY_QUEUE", "gateway_results_queue")
    monkeypatch.setenv("FILTER_DATE_QUEUE", "filter_date_queue")
    monkeypatch.setenv("FILTER_Q1_QUEUE", "filter_q1_queue")
    monkeypatch.setenv("SUM_Q2_QUEUE", "sum_q2_queue")
    monkeypatch.setenv("FILTER_Q3_QUEUE", "filter_q3_queue")
    monkeypatch.setenv("SCATTER_GATHER_MAPPER_QUEUE", "sg_mapper_queue")
    monkeypatch.setenv("FILTER_Q5_USD_QUEUE", "filter_q5_usd_queue")
    monkeypatch.setenv("SUM_PREFIX", "sum_q3")
    monkeypatch.setenv("SUM_Q3_QUEUE", "sum_q3_queue")
    monkeypatch.setenv("FILTER_AMOUNT", "1")
    monkeypatch.setenv("FILTER_PREFIX", "filter")
    monkeypatch.setenv("USD_ENABLE_Q1", "0")
    monkeypatch.setenv("USD_ENABLE_Q2", "1")
    monkeypatch.setenv("USD_ENABLE_DATE", "0")
    monkeypatch.setenv("DATE_ENABLE_Q3", "0")
    monkeypatch.setenv("DATE_ENABLE_Q4", "0")

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
    sys.modules.pop("workers.filter.filters", None)
    module = importlib.import_module("workers.filter.filters")
    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", FakeQueue)
    monkeypatch.setattr(
        module.middleware,
        "MessageMiddlewareExchangeRabbitMQ",
        FakeExchange,
    )
    return module


def _tx(amount: float, currency: str, fmt: str = "Wire") -> Transaction:
    return Transaction(
        date="2022/09/01 00:08",
        from_bank="1",
        from_account="abc",
        to_bank="2",
        to_account="def",
        amount=amount,
        currency=currency,
        format=fmt,
    )


def _data_packet(client_id: int, transactions: list[Transaction]) -> bytes:
    payload = TransactionSerializer.serialize_batch(transactions)
    return InternalProtocol.create_packet(
        msg_type=MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=payload,
    )


def test_usd_filter_processes_batched_payload(monkeypatch):
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_MAX_TX", "2")
    module = _import_filter_module(monkeypatch, configuration="USD")
    worker = module.FilterWorker()

    transactions = [
        _tx(10.0, "US Dollar"),
        _tx(20.0, "Euro"),
        _tx(30.0, "US Dollar"),
    ]
    message = _data_packet(client_id=42, transactions=transactions)

    worker._process_data_message(message)

    assert worker.processed_by_client[42] == 3
    assert worker.forwarded_by_client[42] == 2

    sum_q2_output = worker.output_queues["sum_q2_queue"]
    assert len(sum_q2_output.sent) == 1

    msg_type, client_id, payload = InternalProtocol.unpack_packet(sum_q2_output.sent[0])
    assert msg_type == MessageType.DATA
    assert client_id == 42
    batch_txs = TransactionSerializer.deserialize_batch(payload)
    assert len(batch_txs) == 2
    assert all(tx.currency == "US Dollar" for tx in batch_txs)


def test_usd_filter_buffers_until_flush(monkeypatch):
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_MAX_TX", "1000")
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_BYTES", str(10 * 1024 * 1024))
    module = _import_filter_module(monkeypatch, configuration="USD")
    worker = module.FilterWorker()

    message = _data_packet(client_id=7, transactions=[_tx(15.0, "US Dollar")])
    worker._process_data_message(message)

    assert worker.processed_by_client[7] == 1
    assert worker.forwarded_by_client[7] == 1
    assert len(worker.output_queues["sum_q2_queue"].sent) == 0

    worker._flush_batcher_for_client(7)
    sent = worker.output_queues["sum_q2_queue"].sent
    assert len(sent) == 1
    msg_type, client_id, payload = InternalProtocol.unpack_packet(sent[0])
    assert msg_type == MessageType.DATA
    assert client_id == 7
    txs = TransactionSerializer.deserialize_batch(payload)
    assert len(txs) == 1 and txs[0].currency == "US Dollar"
