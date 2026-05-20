import importlib
import sys
import types

from common.domain.transaction import Transaction
from common.message_protocol.common import ControlMessage, MessageType
from common.message_protocol.control_message_serializer import ControlMessageSerializer
from common.message_protocol.transaction_serializer import TransactionSerializer


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
