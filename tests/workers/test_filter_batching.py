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


def _import_filter_module(
    monkeypatch, configuration: str = "USD", filter_amount: str = "1"
):
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
    monkeypatch.setenv("FILTER_AMOUNT", filter_amount)
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


def test_q5_filter_batches_wire_and_ach_to_filter_q5_usd(monkeypatch):
    # filter_q5_format (C_Q5) tambien debe acumular en el batcher.
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_MAX_TX", "2")
    module = _import_filter_module(monkeypatch, configuration="Q5")
    worker = module.FilterWorker()

    transactions = [
        _tx(1.0, "US Dollar", fmt="Wire"),
        _tx(2.0, "US Dollar", fmt="Credit Card"),  # no pasa filtro Q5
        _tx(3.0, "Euro", fmt="ACH"),
    ]
    worker._process_data_message(_data_packet(99, transactions))

    assert worker.processed_by_client[99] == 3
    assert worker.forwarded_by_client[99] == 2

    q5_output = worker.output_queues["filter_q5_usd_queue"]
    assert len(q5_output.sent) == 1

    _, _, payload = InternalProtocol.unpack_packet(q5_output.sent[0])
    batch_txs = TransactionSerializer.deserialize_batch(payload)
    assert len(batch_txs) == 2
    assert {tx.format for tx in batch_txs} == {"Wire", "ACH"}


def test_eof_flushes_partial_batch_before_forwarding(monkeypatch):
    # Sin llegar al limite, una unica tx queda en el batcher hasta que el
    # EOF dispara _try_forward_single_filter_eof, que debe flushear antes
    # de forwardear el EOF downstream.
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_MAX_TX", "1000")
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_BYTES", str(10 * 1024 * 1024))
    module = _import_filter_module(monkeypatch, configuration="USD")
    worker = module.FilterWorker()

    # 1 DATA con 1 USD tx -> queda en buffer.
    worker._process_data_message(_data_packet(11, [_tx(5.0, "US Dollar")]))
    sum_q2_output = worker.output_queues["sum_q2_queue"]
    assert len(sum_q2_output.sent) == 0

    # EOF: expected_total = 1 (las DATA que procesaron upstream). El filter
    # tiene processed = 1, asi que _try_forward_single_filter_eof dispara
    # el flush + el forward del EOF.
    control_payload = module.message_protocol.internal.ControlMessageSerializer.serialize(
        module.message_protocol.internal.ControlMessage(
            sender_id=0, expected_total=1, processed_count=0
        )
    )
    eof_message = InternalProtocol.create_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=(11).to_bytes(16, byteorder="big"),
        payload=control_payload,
    )
    worker._process_data_message(eof_message)

    # Tras el EOF: 2 publishes en sum_q2_queue: primero el batch (DATA con 1 tx),
    # despues el EOF.
    assert len(sum_q2_output.sent) == 2
    data_msg_type, _, data_payload = InternalProtocol.unpack_packet(sum_q2_output.sent[0])
    eof_msg_type, _, _ = InternalProtocol.unpack_packet(sum_q2_output.sent[1])
    assert data_msg_type == MessageType.DATA
    assert eof_msg_type == MessageType.EOF
    # El batch DATA debe contener la 1 tx pendiente.
    assert len(TransactionSerializer.deserialize_batch(data_payload)) == 1


def test_batcher_isolates_buffers_between_clients(monkeypatch):
    # Dos clientes mandando DATA concurrentes no se cruzan en el batcher.
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_MAX_TX", "1000")
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_BYTES", str(10 * 1024 * 1024))
    module = _import_filter_module(monkeypatch, configuration="USD")
    worker = module.FilterWorker()

    worker._process_data_message(_data_packet(1, [_tx(1.0, "US Dollar")]))
    worker._process_data_message(_data_packet(2, [_tx(2.0, "US Dollar")]))

    assert worker.forwarded_by_client[1] == 1
    assert worker.forwarded_by_client[2] == 1
    # Nada se publico (limites altos, sin EOF).
    assert len(worker.output_queues["sum_q2_queue"].sent) == 0

    # Flushear solo client 1 no toca el buffer del client 2.
    worker._flush_batcher_for_client(1)
    sent = worker.output_queues["sum_q2_queue"].sent
    assert len(sent) == 1
    _, client_id, payload = InternalProtocol.unpack_packet(sent[0])
    assert client_id == 1
    assert TransactionSerializer.deserialize_batch(payload)[0].amount == 1.0

    worker._flush_batcher_for_client(2)
    sent_now = worker.output_queues["sum_q2_queue"].sent
    assert len(sent_now) == 2
    _, client_id, payload = InternalProtocol.unpack_packet(sent_now[1])
    assert client_id == 2
    assert TransactionSerializer.deserialize_batch(payload)[0].amount == 2.0


def test_control_path_uses_thread_local_output_publishers(monkeypatch):
    module = _import_filter_module(
        monkeypatch,
        configuration="Q1",
        filter_amount="2",
    )
    worker = module.FilterWorker()

    client_id = 123
    worker.forwarded_by_client[client_id] = 3
    thread_control_output = FakeExchange()
    thread_output_queues = {
        "gateway_results_queue": FakeQueue(),
    }

    control_payload = module.message_protocol.internal.ControlMessageSerializer.serialize(
        module.message_protocol.internal.ControlMessage(
            sender_id=1, expected_total=0, processed_count=2
        )
    )
    flush_ack = InternalProtocol.create_packet(
        msg_type=MessageType.FLUSH_ACK,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=control_payload,
    )

    worker._process_control_message(
        flush_ack,
        control_output=thread_control_output,
        output_queues=thread_output_queues,
    )

    assert len(worker.output_queues["gateway_results_queue"].sent) == 0
    assert len(thread_output_queues["gateway_results_queue"].sent) == 1
    msg_type, sent_client_id, payload = InternalProtocol.unpack_packet(
        thread_output_queues["gateway_results_queue"].sent[0]
    )
    forwarded_eof = module.message_protocol.internal.ControlMessageSerializer.deserialize(
        payload
    )
    assert msg_type == MessageType.EOF
    assert sent_client_id == client_id
    assert forwarded_eof.expected_total == 5


def test_control_path_uses_thread_local_control_publisher(monkeypatch):
    module = _import_filter_module(
        monkeypatch,
        configuration="Q1",
        filter_amount="2",
    )
    worker = module.FilterWorker()

    client_id = 456
    worker.processed_by_client[client_id] = 3
    thread_control_output = FakeExchange()
    thread_output_queues = {
        "gateway_results_queue": FakeQueue(),
    }

    control_payload = module.message_protocol.internal.ControlMessageSerializer.serialize(
        module.message_protocol.internal.ControlMessage(
            sender_id=1, expected_total=5, processed_count=2
        )
    )
    processed_answer = InternalProtocol.create_packet(
        msg_type=MessageType.PROCESSED_ANSWER,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=control_payload,
    )

    worker._process_control_message(
        processed_answer,
        control_output=thread_control_output,
        output_queues=thread_output_queues,
    )

    assert len(worker.control_output.sent) == 0
    assert len(thread_control_output.sent) == 1
    msg_type, sent_client_id, _ = InternalProtocol.unpack_packet(
        thread_control_output.sent[0]
    )
    assert msg_type == MessageType.FLUSH_ORDER
    assert sent_client_id == client_id
