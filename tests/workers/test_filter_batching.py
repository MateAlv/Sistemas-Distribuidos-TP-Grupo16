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


class FakeShardedPublisher(FakeQueue):
    def __init__(self, host, exchange_name, routing_key_prefix, shard_count):
        super().__init__()
        self.host = host
        self.exchange_name = exchange_name
        self.routing_key_prefix = routing_key_prefix
        self.shard_count = shard_count


def _import_filter_module(
    monkeypatch,
    configuration: str = "USD",
    filter_amount: str = "1",
    usd_enable_q1: str = "0",
    usd_enable_q2: str = "1",
    usd_enable_date: str = "0",
    date_enable_q3: str = "0",
    date_enable_q4: str = "0",
):
    # Solo activamos USD_ENABLE_Q2 para acotar el output del filter al sum_q2.
    # El resto se desactiva con flags.
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("CONFIGURATION", configuration)
    monkeypatch.setenv("INPUT_QUEUE", "filter_usd_queue")
    monkeypatch.setenv("GATEWAY_QUEUE", "gateway_results_queue")
    monkeypatch.setenv("FILTER_DATE_QUEUE", "filter_date_queue")
    monkeypatch.setenv("FILTER_Q1_QUEUE", "filter_q1_queue")
    monkeypatch.setenv("SUM_Q2_EXCHANGE", "sum_q2_exchange")
    monkeypatch.setenv("SUM_Q2_ROUTING_PREFIX", "sum_q2")
    monkeypatch.setenv("SUM_Q2_AMOUNT", "1")
    monkeypatch.setenv("FILTER_Q3_QUEUE", "filter_q3_queue")
    monkeypatch.setenv("SCATTER_GATHER_MAPPER_QUEUE", "sg_mapper_queue")
    monkeypatch.setenv("FILTER_Q5_USD_QUEUE", "filter_q5_usd_queue")
    monkeypatch.setenv("SUM_PREFIX", "sum_q3")
    monkeypatch.setenv("SUM_Q3_QUEUE", "sum_q3_queue")
    monkeypatch.setenv("FILTER_AMOUNT", filter_amount)
    monkeypatch.setenv("FILTER_PREFIX", "filter")
    monkeypatch.setenv("USD_ENABLE_Q1", usd_enable_q1)
    monkeypatch.setenv("USD_ENABLE_Q2", usd_enable_q2)
    monkeypatch.setenv("USD_ENABLE_DATE", usd_enable_date)
    monkeypatch.setenv("DATE_ENABLE_Q3", date_enable_q3)
    monkeypatch.setenv("DATE_ENABLE_Q4", date_enable_q4)

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
    monkeypatch.setattr(module.middleware, "LazyQueue", FakeQueue)
    monkeypatch.setattr(module.middleware, "ShardedByClientPublisher", FakeShardedPublisher)
    return module


def _tx(
    amount: float,
    currency: str,
    fmt: str = "Wire",
    date: str = "2022/09/01 00:08",
    from_bank: str = "1",
    from_account: str = "abc",
) -> Transaction:
    return Transaction(
        date=date,
        from_bank=from_bank,
        from_account=from_account,
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

    sum_q2_output = worker.output_queues["sum_q2"]
    assert sum_q2_output.exchange_name == "sum_q2_exchange"
    assert sum_q2_output.routing_key_prefix == "sum_q2"
    assert sum_q2_output.shard_count == 1

    transactions = [
        _tx(10.0, "US Dollar"),
        _tx(20.0, "Euro"),
        _tx(30.0, "US Dollar"),
    ]
    message = _data_packet(client_id=42, transactions=transactions)

    worker._process_data_message(message)

    assert worker.processed_by_client[42] == 3
    assert worker.forwarded_by_client[42] == 2

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
    assert len(worker.output_queues["sum_q2"].sent) == 0

    worker._flush_batcher_for_client(7)
    sent = worker.output_queues["sum_q2"].sent
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


def test_date_filter_uses_notebook_q3_timestamp_bounds(monkeypatch):
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_MAX_TX", "1")
    module = _import_filter_module(
        monkeypatch,
        configuration="DATE",
        usd_enable_q2="0",
        date_enable_q3="1",
        date_enable_q4="0",
    )
    worker = module.FilterWorker()

    worker._process_data_message(
        _data_packet(
            314,
            [
                _tx(
                    10.0,
                    "US Dollar",
                    date="2022/09/05 23:59",
                    from_account="baseline",
                ),
                _tx(
                    20.0,
                    "US Dollar",
                    date="2022/09/06 00:00",
                    from_account="start",
                ),
                _tx(
                    30.0,
                    "US Dollar",
                    date="2022/09/14 23:59",
                    from_account="end",
                ),
                _tx(
                    40.0,
                    "US Dollar",
                    date="2022/09/15 00:00",
                    from_account="excluded",
                ),
                _tx(
                    50.0,
                    "US Dollar",
                    date="2022/08/31 23:59",
                    from_account="before",
                ),
            ],
        )
    )

    sum_q3_sent = worker.output_queues["sum_q3_queue"].sent
    q3_candidates_sent = worker.output_queues["filter_q3_queue"].sent
    assert len(sum_q3_sent) == 1
    assert len(q3_candidates_sent) == 2

    _, _, baseline_payload = InternalProtocol.unpack_packet(sum_q3_sent[0])
    baseline_txs = TransactionSerializer.deserialize_batch(baseline_payload)
    assert [tx.from_account for tx in baseline_txs] == ["baseline"]

    candidate_accounts = []
    for packet in q3_candidates_sent:
        _, _, payload = InternalProtocol.unpack_packet(packet)
        candidate_accounts.extend(
            tx.from_account for tx in TransactionSerializer.deserialize_batch(payload)
        )
    assert candidate_accounts == ["start", "end"]


def test_date_filter_uses_notebook_q4_timestamp_bounds(monkeypatch):
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_MAX_TX", "1")
    module = _import_filter_module(
        monkeypatch,
        configuration="DATE",
        usd_enable_q2="0",
        date_enable_q3="0",
        date_enable_q4="1",
    )
    worker = module.FilterWorker()

    worker._process_data_message(
        _data_packet(
            315,
            [
                _tx(
                    10.0,
                    "US Dollar",
                    date="2022/09/05 23:59",
                    from_account="included-5",
                ),
                _tx(
                    20.0,
                    "US Dollar",
                    date="2022/09/06",
                    from_account="included-bound",
                ),
                _tx(
                    30.0,
                    "US Dollar",
                    date="2022/09/06 00:00",
                    from_account="excluded-time",
                ),
                _tx(
                    40.0,
                    "US Dollar",
                    date="2022/08/31 23:59",
                    from_account="excluded-before",
                ),
            ],
        )
    )

    q4_sent = worker.output_queues["sg_mapper_queue"].sent
    assert len(q4_sent) == 2

    q4_accounts = []
    for packet in q4_sent:
        _, _, payload = InternalProtocol.unpack_packet(packet)
        q4_accounts.extend(
            tx.from_account for tx in TransactionSerializer.deserialize_batch(payload)
        )
    assert q4_accounts == ["included-5", "included-bound"]


def test_date_filter_routes_q4_to_source_prefilter_exchange_with_global_eof(
    monkeypatch,
):
    monkeypatch.setenv("FILTER_OUTPUT_BATCH_MAX_TX", "1")
    monkeypatch.setenv("Q4_FILTER_INPUT_EXCHANGE", "q4_prefilter")
    monkeypatch.setenv("Q4_FILTER_INPUT_ROUTING_PREFIX", "q4_source")
    monkeypatch.setenv("Q4_FILTER_AMOUNT", "2")
    module = _import_filter_module(
        monkeypatch,
        configuration="DATE",
        usd_enable_q2="0",
        date_enable_q3="0",
        date_enable_q4="1",
    )
    worker = module.FilterWorker()

    txs = [
        _tx(
            10.0,
            "US Dollar",
            date="2022/09/05 23:59",
            from_bank="001",
            from_account="source-a",
        ),
        _tx(
            20.0,
            "US Dollar",
            date="2022/09/06",
            from_bank="002",
            from_account="source-b",
        ),
    ]

    worker._process_data_message(_data_packet(316, txs))

    assert set(worker.output_queues) == {"q4_source_0", "q4_source_1"}
    q4_accounts_by_key = {}
    for key, output in worker.output_queues.items():
        accounts = []
        for packet in output.sent:
            msg_type, _, payload = InternalProtocol.unpack_packet(packet)
            if msg_type != MessageType.DATA:
                continue
            accounts.extend(
                tx.from_account
                for tx in TransactionSerializer.deserialize_batch(payload)
            )
        q4_accounts_by_key[key] = accounts

    for tx in txs:
        expected_key = worker._q4_filter_output_for_transaction(tx)
        assert tx.from_account in q4_accounts_by_key[expected_key]

    control_payload = module.message_protocol.internal.ControlMessageSerializer.serialize(
        module.message_protocol.internal.ControlMessage(
            sender_id=0, expected_total=2, processed_count=0
        )
    )
    eof_message = InternalProtocol.create_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=(316).to_bytes(16, byteorder="big"),
        payload=control_payload,
    )
    worker._process_data_message(eof_message)

    for output in worker.output_queues.values():
        eof_packets = [
            packet
            for packet in output.sent
            if InternalProtocol.unpack_packet(packet)[0] == MessageType.EOF
        ]
        assert len(eof_packets) == 1
        _, _, payload = InternalProtocol.unpack_packet(eof_packets[0])
        control = module.message_protocol.internal.ControlMessageSerializer.deserialize(
            payload
        )
        assert control.expected_total == 2


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
    sum_q2_output = worker.output_queues["sum_q2"]
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

    # Tras el EOF: 2 publishes en sum_q2: primero el batch (DATA con 1 tx),
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
    assert len(worker.output_queues["sum_q2"].sent) == 0

    # Flushear solo client 1 no toca el buffer del client 2.
    worker._flush_batcher_for_client(1)
    sent = worker.output_queues["sum_q2"].sent
    assert len(sent) == 1
    _, client_id, payload = InternalProtocol.unpack_packet(sent[0])
    assert client_id == 1
    assert TransactionSerializer.deserialize_batch(payload)[0].amount == 1.0

    worker._flush_batcher_for_client(2)
    sent_now = worker.output_queues["sum_q2"].sent
    assert len(sent_now) == 2
    _, client_id, payload = InternalProtocol.unpack_packet(sent_now[1])
    assert client_id == 2
    assert TransactionSerializer.deserialize_batch(payload)[0].amount == 2.0


def test_response_path_uses_thread_local_output_publishers(monkeypatch):
    """_handle_flush_ack must emit EOF to thread-local output_queues, not self.output_queues."""
    import json

    module = _import_filter_module(
        monkeypatch,
        configuration="Q1",
        filter_amount="2",
    )
    worker = module.FilterWorker()

    client_id = 123
    # Leader (worker 0) already processed 3 forwarded items for this client.
    worker.forwarded_by_client[client_id] = 3
    worker.forwarded_by_output_by_client[client_id] = {"gateway_results_queue": 3}
    # Mark the coordinator as having started a broadcast round for this client.
    worker.coordinator._leader_expected[client_id] = 5

    thread_output_queues = {"gateway_results_queue": FakeQueue()}

    # JSON FLUSH_ACK from worker 1 carrying 2 forwarded items.
    flush_ack_payload = json.dumps(
        {"sender_id": 1, "forwarded_by_output": {"gateway_results_queue": 2}}
    ).encode("utf-8")

    worker._handle_flush_ack(client_id, flush_ack_payload, thread_output_queues)

    # EOF must go to the thread-local output, not the data-thread's own queues.
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
    assert forwarded_eof.expected_total == 5  # 3 leader + 2 non-leader


def test_response_path_broadcasts_flush_order_via_thread_local_senders(monkeypatch):
    """When all PROCESSED_ANSWERs arrive, FLUSH_ORDER must go through thread-local
    control_senders, not through self._main_control_senders."""
    from common.message_protocol.internal.common import MessageType as MT

    module = _import_filter_module(
        monkeypatch,
        configuration="Q1",
        filter_amount="2",
    )
    worker = module.FilterWorker()

    client_id = 456
    # Simulate coordinator state: leader has expected=5, worker 1 already answered
    # with processed_count=2.  Only worker 0's own answer is outstanding.
    worker.coordinator._leader_expected[client_id] = 5
    worker.coordinator._leader_processed[client_id] = 2
    worker.coordinator._leader_responders[client_id] = {1}
    worker.processed_by_client[client_id] = 3

    thread_control_senders = {
        worker.coordinator.control_queue_for(0): FakeQueue(),
        worker.coordinator.control_queue_for(1): FakeQueue(),
    }
    thread_output_queues = {"gateway_results_queue": FakeQueue()}

    # PROCESSED_ANSWER from worker 0 (the leader, self-reporting count=3)
    ctrl = module.message_protocol.internal.ControlMessage(
        sender_id=0, expected_total=5, processed_count=3
    )
    ctrl_bytes = module.message_protocol.internal.ControlMessageSerializer.serialize(ctrl)
    processed_answer = InternalProtocol.create_packet(
        msg_type=MessageType.PROCESSED_ANSWER,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=ctrl_bytes,
    )

    worker._handle_response(
        processed_answer,
        lambda: None,  # ack
        lambda: None,  # nack
        thread_control_senders,
        thread_output_queues,
    )

    # FLUSH_ORDER must be sent via thread-local control senders, not main senders.
    for q in worker._main_control_senders.values():
        assert len(q.sent) == 0, "FLUSH_ORDER must not use _main_control_senders"
    for q in thread_control_senders.values():
        assert len(q.sent) == 1
        msg_type, sent_client_id, _ = InternalProtocol.unpack_packet(q.sent[0])
        assert msg_type == MessageType.FLUSH_ORDER
        assert sent_client_id == client_id
