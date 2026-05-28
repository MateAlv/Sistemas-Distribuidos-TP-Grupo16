import importlib
import sys
import types

from common.domain.transaction import Transaction
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal.scatter_gather_serializer import (
    ScatterGatherRelationSerializer,
    ScatterGatherResultSerializer,
)
from common.message_protocol.internal.transaction_serializer import TransactionSerializer


class FakeQueue:
    def __init__(self, *args, **kwargs):
        self.sent = []

    def send(self, message):
        self.sent.append(message)

    def start_consuming(self, callback):
        pass

    def stop_consuming(self):
        pass

    def close(self):
        pass


class FakeExchange(FakeQueue):
    pass


def _install_fake_pika(monkeypatch):
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


def _import_module(monkeypatch, module_name: str, env: dict[str, str]):
    _install_fake_pika(monkeypatch)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)
    if hasattr(module, "MessageMiddlewareQueueRabbitMQ"):
        monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", FakeQueue)
    if hasattr(module, "MessageMiddlewareExchangeRabbitMQ"):
        monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    return module


def _control_payload(sender_id: int, expected_total: int, processed_count: int) -> bytes:
    return ControlMessageSerializer.serialize(
        ControlMessage(
            sender_id=sender_id,
            expected_total=expected_total,
            processed_count=processed_count,
        )
    )


def test_mapper_forwards_eof_after_all_mapper_reports(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.mapper.mapper",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "INPUT_QUEUE": "sg_mapper_queue",
            "SG_LINKER_EXCHANGE": "sg_linker_exchange",
            "SG_LINKER_AMOUNT": "1",
            "SG_MAPPER_AMOUNT": "2",
            "SG_MAPPER_PREFIX": "sg_mapper",
        },
    )
    worker = module.ScatterGatherMapper()
    client_id = 17
    events = []

    worker._leader_expected_by_client[client_id] = 2

    first_report = worker._packet(
        MessageType.PROCESSED_ANSWER,
        client_id,
        _control_payload(sender_id=0, expected_total=2, processed_count=1),
    )
    worker._handle_leader_report(
        first_report,
        ack=lambda: events.append("ack"),
        nack=lambda: events.append("nack"),
    )

    assert events == ["ack"]
    assert worker._linkers[0].sent == []

    second_report = worker._packet(
        MessageType.PROCESSED_ANSWER,
        client_id,
        _control_payload(sender_id=1, expected_total=2, processed_count=1),
    )
    worker._handle_leader_report(
        second_report,
        ack=lambda: events.append("ack"),
        nack=lambda: events.append("nack"),
    )

    assert events == ["ack", "ack"]
    assert len(worker._linkers[0].sent) == 1
    msg_type, received_client_id, payload = worker._proto.unpack_packet(
        worker._linkers[0].sent[0]
    )
    control = ControlMessageSerializer.deserialize(payload)
    assert msg_type == MessageType.EOF
    assert received_client_id == client_id
    assert control.sender_id == 0
    assert control.expected_total == 4
    assert client_id in worker._closed_by_client


def test_mapper_reports_late_data_after_pending_eof(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.mapper.mapper",
        {
            "ID": "1",
            "MOM_HOST": "rabbitmq",
            "INPUT_QUEUE": "sg_mapper_queue",
            "SG_LINKER_EXCHANGE": "sg_linker_exchange",
            "SG_LINKER_AMOUNT": "1",
            "SG_MAPPER_AMOUNT": "2",
            "SG_MAPPER_PREFIX": "sg_mapper",
        },
    )
    worker = module.ScatterGatherMapper()
    client_id = 23
    reports = []
    tx = Transaction(
        date="2022-09-02",
        from_bank="bank-a",
        from_account="A",
        to_bank="bank-m",
        to_account="M",
        amount=10.0,
        currency="US Dollar",
        format="Wire",
    )

    worker._pending_eof_by_client[client_id] = (2, 0)
    monkeypatch.setattr(
        worker,
        "_report_to_leader",
        lambda *args, **kwargs: reports.append((args, kwargs)),
    )

    worker._handle_data_packet(client_id, TransactionSerializer.serialize(tx))

    assert len(worker._linkers[0].sent) == 2
    assert reports == [
        (
            (client_id, 0),
            {"processed_count": 1, "forwarded_count": 2},
        )
    ]


def test_mapper_accepts_batched_transactions(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.mapper.mapper",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "INPUT_QUEUE": "sg_mapper_queue",
            "SG_LINKER_EXCHANGE": "sg_linker_exchange",
            "SG_LINKER_AMOUNT": "1",
            "SG_MAPPER_AMOUNT": "1",
            "SG_MAPPER_PREFIX": "sg_mapper",
        },
    )
    worker = module.ScatterGatherMapper()
    client_id = 24
    transactions = [
        Transaction(
            date="2022-09-02",
            from_bank="bank-a",
            from_account="A1",
            to_bank="bank-m",
            to_account="M1",
            amount=10.0,
            currency="US Dollar",
            format="Wire",
        ),
        Transaction(
            date="2022-09-03",
            from_bank="bank-a",
            from_account="A2",
            to_bank="bank-m",
            to_account="M2",
            amount=20.0,
            currency="US Dollar",
            format="ACH",
        ),
    ]

    worker._handle_data_packet(
        client_id, TransactionSerializer.serialize_batch(transactions)
    )

    assert worker._processed_by_client[client_id] == 2
    assert worker._forwarded_by_client[client_id] == 4
    # Edges are buffered, not published one-by-one: nothing is sent until a
    # flush (size threshold or EOF).
    assert worker._linkers[0].sent == []

    worker._flush_client_buffers(client_id)

    # With a single linker partition the two edge tags produce two batched
    # messages, each carrying both transactions.
    assert len(worker._linkers[0].sent) == 2
    tags = set()
    for raw in worker._linkers[0].sent:
        msg_type, received_client_id, payload = worker._proto.unpack_packet(raw)
        assert msg_type == MessageType.DATA
        assert received_client_id == client_id
        tags.add(payload[0])
        batch = TransactionSerializer.deserialize_batch(payload[1:])
        assert len(batch) == 2
    assert tags == {1, 2}


def test_mapper_leader_report_can_use_thread_local_linkers(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.mapper.mapper",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "INPUT_QUEUE": "sg_mapper_queue",
            "SG_LINKER_EXCHANGE": "sg_linker_exchange",
            "SG_LINKER_AMOUNT": "1",
            "SG_MAPPER_AMOUNT": "2",
            "SG_MAPPER_PREFIX": "sg_mapper",
        },
    )
    worker = module.ScatterGatherMapper()
    client_id = 25
    thread_linkers = [FakeExchange()]
    worker._leader_expected_by_client[client_id] = 1

    report = worker._packet(
        MessageType.PROCESSED_ANSWER,
        client_id,
        _control_payload(sender_id=1, expected_total=2, processed_count=1),
    )
    worker._handle_leader_report(
        report,
        ack=lambda: None,
        nack=lambda: None,
        linkers=thread_linkers,
    )

    assert worker._linkers[0].sent == []
    assert len(thread_linkers[0].sent) == 1


def test_linker_forwards_on_mapper_group_eof(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.linker.linker",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "SG_LINKER_EXCHANGE": "sg_linker_exchange",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "SG_DETECTOR_AMOUNT": "1",
        },
    )
    worker = module.ScatterGatherLinker()
    client_id = 31
    worker._emitted_count_by_client[client_id] = 3
    worker._emitted_count_by_partition[client_id][0] = 3

    worker._handle_eof(client_id, _control_payload(0, 0, 0))

    assert len(worker._detectors[0].sent) == 1
    msg_type, received_client_id, payload = worker._proto.unpack_packet(
        worker._detectors[0].sent[0]
    )
    control = ControlMessageSerializer.deserialize(payload)
    assert msg_type == MessageType.EOF
    assert received_client_id == client_id
    assert control.sender_id == 0
    assert control.expected_total == 3
    assert client_id in worker._closed_by_client


def test_linker_forwards_after_aggregated_mapper_group_eof(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.linker.linker",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "SG_LINKER_EXCHANGE": "sg_linker_exchange",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "SG_DETECTOR_AMOUNT": "1",
        },
    )
    worker = module.ScatterGatherLinker()
    client_id = 32
    worker._emitted_count_by_client[client_id] = 3
    worker._emitted_count_by_partition[client_id][0] = 3

    worker._handle_eof(client_id, _control_payload(0, 0, 0))

    assert len(worker._detectors[0].sent) == 1
    assert client_id in worker._closed_by_client


def test_linker_emits_each_relation_once_and_dedups_repeated_edges(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.linker.linker",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "SG_LINKER_EXCHANGE": "sg_linker_exchange",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "SG_DETECTOR_AMOUNT": "1",
        },
    )
    worker = module.ScatterGatherLinker()
    client_id = 34

    # A -> M and M -> B form one relation (A, M, B).
    worker._add_incoming(client_id, m="M", a="A")
    worker._add_outgoing(client_id, m="M", b="B")
    assert worker._emitted_count_by_client[client_id] == 1

    # Repeated edges (same accounts, e.g. another transaction or a redelivery)
    # add nothing new to the neighbour sets, so nothing is emitted again.
    worker._add_incoming(client_id, m="M", a="A")
    worker._add_outgoing(client_id, m="M", b="B")
    assert worker._emitted_count_by_client[client_id] == 1

    # A second distinct A pairs only with the existing B: exactly one new
    # relation (A2, M, B), never a duplicate of (A, M, B).
    worker._add_incoming(client_id, m="M", a="A2")
    assert worker._emitted_count_by_client[client_id] == 2

    worker._handle_eof(client_id, _control_payload(0, 0, 0))
    data_type, _, data_payload = worker._proto.unpack_packet(
        worker._detectors[0].sent[0]
    )
    relations = ScatterGatherRelationSerializer.deserialize_batch(data_payload)
    assert data_type == MessageType.DATA
    pairs = {(r.from_account, r.to_account) for r in relations}
    assert pairs == {("A", "B"), ("A2", "B")}


def test_linker_batches_relations_until_eof_flush(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.linker.linker",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "SG_LINKER_EXCHANGE": "sg_linker_exchange",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "SG_DETECTOR_AMOUNT": "1",
            "SG_LINKER_BATCH_MAX_RELATIONS": "100",
        },
    )
    worker = module.ScatterGatherLinker()
    client_id = 33

    worker._emit_relation(client_id, "A1", "M", "B1")
    worker._emit_relation(client_id, "A2", "M", "B2")

    assert worker._detectors[0].sent == []

    worker._handle_eof(client_id, _control_payload(0, 0, 0))

    assert len(worker._detectors[0].sent) == 2
    data_type, _, data_payload = worker._proto.unpack_packet(
        worker._detectors[0].sent[0]
    )
    eof_type, _, eof_payload = worker._proto.unpack_packet(
        worker._detectors[0].sent[1]
    )
    relations = ScatterGatherRelationSerializer.deserialize_batch(data_payload)
    control = ControlMessageSerializer.deserialize(eof_payload)
    assert data_type == MessageType.DATA
    assert eof_type == MessageType.EOF
    assert len(relations) == 2
    assert control.expected_total == 2


def test_detector_waits_for_distinct_linker_eofs(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.detector.detector",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "GATEWAY_Q4_QUEUE": "gateway_q4_results_queue",
            "SG_LINKER_AMOUNT": "2",
        },
    )
    worker = module.ScatterGatherDetector()
    client_id = 37
    worker._emitted[client_id].add(("A", "B"))
    worker._emitted_count_by_client[client_id] = 1

    worker._handle_eof(client_id, _control_payload(0, 0, 0))
    worker._handle_eof(client_id, _control_payload(0, 0, 0))

    assert worker._output.sent == []

    worker._handle_eof(client_id, _control_payload(1, 0, 0))

    assert len(worker._output.sent) == 1
    msg_type, received_client_id, payload = worker._proto.unpack_packet(
        worker._output.sent[0]
    )
    control = ControlMessageSerializer.deserialize(payload)
    assert msg_type == MessageType.EOF
    assert received_client_id == client_id
    assert control.sender_id == 0
    assert control.expected_total == 1
    assert client_id in worker._closed_by_client


def test_detector_leader_broadcasts_after_linker_group_eof(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.detector.detector",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "GATEWAY_Q4_QUEUE": "gateway_q4_results_queue",
            "SG_LINKER_AMOUNT": "2",
            "SG_DETECTOR_AMOUNT": "2",
            "SG_DETECTOR_PREFIX": "sg_detector",
        },
    )
    worker = module.ScatterGatherDetector()
    client_id = 41

    worker._handle_eof(client_id, _control_payload(0, 3, 0))

    assert worker._control_sender.sent == []
    assert worker._output.sent == []

    worker._handle_eof(client_id, _control_payload(1, 4, 0))

    assert len(worker._control_sender.sent) == 1
    assert worker._output.sent == []
    msg_type, received_client_id, payload = worker._proto.unpack_packet(
        worker._control_sender.sent[0]
    )
    control = ControlMessageSerializer.deserialize(payload)
    assert msg_type == MessageType.EOF_RECEIVED
    assert received_client_id == client_id
    assert control.sender_id == 0
    assert control.expected_total == 7


def test_detector_leader_forwards_gateway_eof_after_detector_reports(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.detector.detector",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "GATEWAY_Q4_QUEUE": "gateway_q4_results_queue",
            "SG_LINKER_AMOUNT": "1",
            "SG_DETECTOR_AMOUNT": "3",
            "SG_DETECTOR_PREFIX": "sg_detector",
        },
    )
    worker = module.ScatterGatherDetector()
    client_id = 43
    worker._leader_expected_by_client[client_id] = 3
    worker._leader_emitted_by_client[client_id] = 1

    first_report = worker._packet(
        MessageType.PROCESSED_ANSWER,
        client_id,
        _control_payload(sender_id=1, expected_total=1, processed_count=1),
    )
    worker._handle_leader_report(
        first_report,
        ack=lambda: None,
        nack=lambda: None,
    )

    assert worker._output.sent == []

    second_report = worker._packet(
        MessageType.PROCESSED_ANSWER,
        client_id,
        _control_payload(sender_id=2, expected_total=1, processed_count=2),
    )
    worker._handle_leader_report(
        second_report,
        ack=lambda: None,
        nack=lambda: None,
    )

    assert len(worker._output.sent) == 1
    msg_type, received_client_id, payload = worker._proto.unpack_packet(
        worker._output.sent[0]
    )
    control = ControlMessageSerializer.deserialize(payload)
    assert msg_type == MessageType.EOF
    assert received_client_id == client_id
    assert control.sender_id == 0
    assert control.expected_total == 3
    assert client_id in worker._closed_by_client


def test_detector_reports_late_relations_after_pending_eof(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.detector.detector",
        {
            "ID": "1",
            "MOM_HOST": "rabbitmq",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "GATEWAY_Q4_QUEUE": "gateway_q4_results_queue",
            "SG_LINKER_AMOUNT": "1",
            "SG_DETECTOR_AMOUNT": "2",
            "SG_DETECTOR_PREFIX": "sg_detector",
        },
    )
    worker = module.ScatterGatherDetector()
    client_id = 47
    reports = []
    monkeypatch.setattr(
        worker,
        "_report_to_leader",
        lambda *args, **kwargs: reports.append((args, kwargs)),
    )

    broadcast = worker._packet(
        MessageType.EOF_RECEIVED,
        client_id,
        _control_payload(sender_id=0, expected_total=5, processed_count=0),
    )
    worker._handle_eof_broadcast(
        broadcast,
        ack=lambda: None,
        nack=lambda: None,
    )

    assert reports == []

    for index in range(1, 6):
        worker._add_relation(client_id, "A", f"M{index}", "B")

    worker._handle_eof(client_id, _control_payload(0, 5, 0))

    assert reports == [
        (
            (client_id, 0),
            {"processed_count": 5, "emitted_count": 1},
        )
    ]
    assert len(worker._output.sent) == 1
    msg_type, received_client_id, payload = worker._proto.unpack_packet(
        worker._output.sent[0]
    )
    result = ScatterGatherResultSerializer.deserialize(payload)
    assert msg_type == MessageType.DATA
    assert received_client_id == client_id
    assert result.from_account == "A"
    assert result.to_account == "B"


def test_detector_emits_once_threshold_distinct_intermediaries_reached(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.detector.detector",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "GATEWAY_Q4_QUEUE": "gateway_q4_results_queue",
            "SG_LINKER_AMOUNT": "1",
            "SG_DETECTOR_AMOUNT": "1",
            "MIN_INTERMEDIARIES": "5",
        },
    )
    worker = module.ScatterGatherDetector()
    client_id = 51

    # Four distinct intermediaries: below threshold, nothing emitted yet.
    for index in range(1, 5):
        worker._add_relation(client_id, "A", f"M{index}", "B")
    assert worker._output.sent == []
    # State is a plain count, not a set of accounts (Option A).
    assert worker._intermediaries[client_id][("A", "B")] == 4

    # Fifth distinct intermediary crosses the threshold: emit once, then drop
    # the pair's state.
    worker._add_relation(client_id, "A", "M5", "B")
    assert len(worker._output.sent) == 1
    assert ("A", "B") not in worker._intermediaries[client_id]
    assert ("A", "B") in worker._emitted[client_id]

    _, _, payload = worker._proto.unpack_packet(worker._output.sent[0])
    result = ScatterGatherResultSerializer.deserialize(payload)
    assert (result.from_account, result.to_account) == ("A", "B")


def test_detector_leader_report_can_use_thread_local_gateway_output(monkeypatch):
    module = _import_module(
        monkeypatch,
        "workers.scatter_gather.detector.detector",
        {
            "ID": "0",
            "MOM_HOST": "rabbitmq",
            "SG_DETECTOR_EXCHANGE": "sg_detector_exchange",
            "GATEWAY_Q4_QUEUE": "gateway_q4_results_queue",
            "SG_LINKER_AMOUNT": "1",
            "SG_DETECTOR_AMOUNT": "2",
            "SG_DETECTOR_PREFIX": "sg_detector",
        },
    )
    worker = module.ScatterGatherDetector()
    client_id = 49
    thread_output = FakeQueue()
    worker._leader_expected_by_client[client_id] = 1

    report = worker._packet(
        MessageType.PROCESSED_ANSWER,
        client_id,
        _control_payload(sender_id=1, expected_total=3, processed_count=1),
    )
    worker._handle_leader_report(
        report,
        ack=lambda: None,
        nack=lambda: None,
        output=thread_output,
    )

    assert worker._output.sent == []
    assert len(thread_output.sent) == 1
