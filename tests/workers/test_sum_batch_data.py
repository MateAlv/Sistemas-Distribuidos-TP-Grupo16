"""DATA accounting for the WAL-wired sum worker.

The worker now routes every message through PersistentStateHandler, so DATA
arrives as an addressed packet (sender_id + seq) and the processed count lives
in SumState. These tests drive `_process_message` end-to-end and assert the
durable state, not the old in-worker counters.
"""

import importlib
import sys
import types

from common.domain.transaction import Transaction
from common.message_protocol.internal import (
    InternalProtocol,
    TransactionSerializer,
)
from common.message_protocol.internal.common import MessageType
from common.routing import shard_for_key


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


class FakeExchange(FakeQueue):
    pass


def _noop_ack():
    pass


def _noop_nack(requeue=False):
    pass


def _import_sum_module(monkeypatch, tmp_path, aggregation_amount: str = "1"):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("INPUT_QUEUE", "sum_0")
    monkeypatch.setenv("INPUT_EXCHANGE", "sum_exchange")
    monkeypatch.setenv("INPUT_ROUTING_PREFIX", "sum")
    monkeypatch.setenv("CONFIGURATION", "Q2")
    monkeypatch.setenv("SUM_AMOUNT", "1")
    monkeypatch.setenv("SUM_PREFIX", "sum")
    monkeypatch.setenv("AGGREGATION_AMOUNT", aggregation_amount)
    monkeypatch.setenv("AGGREGATION_PREFIX", "aggregation")
    # Per-test STATE_DIR so the handler never replays a previous run's WAL.
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
    monkeypatch.setattr(module.middleware, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", FakeQueue)
    monkeypatch.setattr(module, "LazyQueue", FakeQueue)
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


def _addressed_data(client_id: int, seq: int, transactions: list[Transaction]) -> bytes:
    return InternalProtocol.create_addressed_packet(
        msg_type=MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=0,
        seq=seq,
        payload=TransactionSerializer.serialize_batch(transactions),
    )


def test_sum_processed_count_scales_with_batch_size(monkeypatch, tmp_path):
    # SumState must count the N transactions in the batch, not 1 per DATA.
    module = _import_sum_module(monkeypatch, tmp_path)
    worker = module.SumWorker()
    client_id = 5

    worker._process_message(
        _addressed_data(client_id, seq=0, transactions=[_tx(10.0), _tx(20.0), _tx(30.0)]),
        _noop_ack,
        _noop_nack,
    )

    assert worker._state.processed_count(client_id) == 3


def test_sum_single_transaction_payload_still_counts_one(monkeypatch, tmp_path):
    module = _import_sum_module(monkeypatch, tmp_path)
    worker = module.SumWorker()

    worker._process_message(
        _addressed_data(9, seq=0, transactions=[_tx(50.0)]),
        _noop_ack,
        _noop_nack,
    )

    assert worker._state.processed_count(9) == 1


def test_sum_duplicate_data_is_ignored(monkeypatch, tmp_path):
    # Same (sender_id, seq): the inbox classifies the redelivery as DONE and the
    # batch is not double-counted.
    module = _import_sum_module(monkeypatch, tmp_path)
    worker = module.SumWorker()
    packet = _addressed_data(7, seq=0, transactions=[_tx(10.0), _tx(20.0)])

    worker._process_message(packet, _noop_ack, _noop_nack)
    worker._process_message(packet, _noop_ack, _noop_nack)  # redelivery

    assert worker._state.processed_count(7) == 2


def test_sum_aggregation_index_uses_shared_routing(monkeypatch, tmp_path):
    module = _import_sum_module(monkeypatch, tmp_path, aggregation_amount="5")
    worker = module.SumWorker()
    partition_key = "bank=014|account=0001"

    assert worker._aggregation_index(partition_key) == shard_for_key(partition_key, 5)
