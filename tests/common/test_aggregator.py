import importlib

from common.constants import C_Q2, C_Q3, C_Q5
from common.domain.partial_result import Q2BankMaxPartial, Q3PaymentFormatPartial
from common.message_protocol.internal.aggregation_serializer import AggregationSerializer
from common.message_protocol.internal.common import MessageType
from common.message_protocol.internal.common.control_message import ControlMessage
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.message_protocol.internal.partial_result_serializer import (
    Q2BankMaxPartialSerializer,
    Q3AverageResultSerializer,
    Q3PaymentFormatPartialSerializer,
)


class _Calls:
    def __init__(self):
        self.acks = 0
        self.nacks = []

    def ack(self):
        self.acks += 1

    def nack(self, requeue=False):
        self.nacks.append(requeue)


class _CollectingSender:
    def __init__(self, destination, sent):
        self.destination = destination
        self.sent = sent

    def send(self, packet: bytes):
        self.sent.append((self.destination, packet))


def _aggregator_module(monkeypatch, tmp_path, configuration):
    monkeypatch.setenv("ID", "1")
    monkeypatch.setenv("MOM_HOST", "localhost")
    monkeypatch.setenv("CONFIGURATION", configuration)
    monkeypatch.setenv("AGGREGATION_PREFIX", "test_aggregation")
    monkeypatch.setenv("AGGREGATION_AMOUNT", "1")
    monkeypatch.setenv("OUTPUT_QUEUE", "test_output_queue")
    monkeypatch.setenv("STATE_DIR", str(tmp_path))

    module = importlib.import_module("workers.aggregator.aggregators")
    return importlib.reload(module)


def _worker_with_captured_outputs(module):
    worker = module.AggregatorWorker()
    sent = []
    senders = {}

    def sender_for(destination):
        if destination not in senders:
            senders[destination] = _CollectingSender(destination, sent)
        return senders[destination]

    worker._tl_sender = sender_for
    return worker, sent


def _addressed_packet(worker, msg_type, client_id, sender_id, seq, payload):
    return worker._internal_protocol.create_addressed_packet(
        msg_type=msg_type,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=sender_id,
        seq=seq,
        payload=payload,
    )


def _send_data(worker, calls, client_id: int, seq: int, payload: bytes) -> None:
    packet = _addressed_packet(worker, MessageType.DATA, client_id, 0, seq, payload)
    worker._process_data_message(packet, calls.ack, calls.nack)


def _send_eof(worker, calls, client_id: int, seq: int, expected_total: int) -> None:
    payload = ControlMessageSerializer.serialize(
        ControlMessage(sender_id=0, expected_total=expected_total, processed_count=0)
    )
    packet = _addressed_packet(worker, MessageType.EOF, client_id, 0, seq, payload)
    worker._process_data_message(packet, calls.ack, calls.nack)


def _close_worker(worker):
    worker._handler.wal.close()


def _run_data_then_eof(worker, client_id, payloads):
    calls = _Calls()
    for seq, payload in enumerate(payloads):
        _send_data(worker, calls, client_id, seq, payload)
    _send_eof(worker, calls, client_id, len(payloads), len(payloads))
    assert calls.nacks == []
    assert calls.acks == len(payloads) + 1


def _unpack_output(worker, packet):
    return worker._internal_protocol.unpack_addressed_packet(packet)


def test_q5_counts_each_data_message(monkeypatch, tmp_path):
    module = _aggregator_module(monkeypatch, tmp_path, C_Q5)
    worker, sent = _worker_with_captured_outputs(module)
    try:
        _run_data_then_eof(
            worker,
            client_id=1,
            payloads=[b"any-transaction-payload"] * 5,
        )

        assert [destination for destination, _ in sent] == ["test_output_queue"] * 2
        data_packet, eof_packet = [packet for _, packet in sent]

        msg_type, client_id, sender_id, seq, payload = _unpack_output(worker, data_packet)
        assert (msg_type, client_id, sender_id, seq) == (MessageType.DATA, 1, 1, 0)
        assert AggregationSerializer.deserialize(payload) == 5

        msg_type, client_id, sender_id, seq, payload = _unpack_output(worker, eof_packet)
        assert (msg_type, client_id, sender_id, seq) == (MessageType.EOF, 1, 1, 1)
        ctrl = ControlMessageSerializer.deserialize(payload)
        assert ctrl.expected_total == 1
    finally:
        _close_worker(worker)


def test_duplicate_eof_and_late_data_are_ignored(monkeypatch, tmp_path):
    module = _aggregator_module(monkeypatch, tmp_path, C_Q5)
    worker, sent = _worker_with_captured_outputs(module)
    try:
        _run_data_then_eof(worker, client_id=7, payloads=[b"tx"] * 3)
        assert len(sent) == 2

        calls = _Calls()
        _send_eof(worker, calls, client_id=7, seq=3, expected_total=3)
        _send_data(worker, calls, client_id=7, seq=4, payload=b"late-tx")
        _send_eof(worker, calls, client_id=7, seq=5, expected_total=3)

        assert calls.nacks == []
        assert calls.acks == 3
        assert len(sent) == 2
    finally:
        _close_worker(worker)


def test_q2_keeps_max_per_bank(monkeypatch, tmp_path):
    module = _aggregator_module(monkeypatch, tmp_path, C_Q2)
    worker, sent = _worker_with_captured_outputs(module)
    try:
        partials = [
            Q2BankMaxPartial(bank_id="X", from_account="a1", amount=10.0),
            Q2BankMaxPartial(bank_id="X", from_account="a2", amount=30.0),
            Q2BankMaxPartial(bank_id="Z", from_account="a3", amount=20.0),
        ]
        _run_data_then_eof(
            worker,
            client_id=2,
            payloads=[Q2BankMaxPartialSerializer.serialize(p) for p in partials],
        )

        assert [destination for destination, _ in sent] == ["test_output_queue"] * 3
        data_packets = [packet for _, packet in sent[:-1]]
        eof_packet = sent[-1][1]

        max_by_bank = {}
        for packet in data_packets:
            msg_type, client_id, sender_id, _seq, payload = _unpack_output(worker, packet)
            assert (msg_type, client_id, sender_id) == (MessageType.DATA, 2, 1)
            result = Q2BankMaxPartialSerializer.deserialize(payload)
            max_by_bank[result.bank_id] = result.amount

        assert max_by_bank == {"X": 30.0, "Z": 20.0}

        msg_type, client_id, sender_id, seq, payload = _unpack_output(worker, eof_packet)
        assert (msg_type, client_id, sender_id, seq) == (MessageType.EOF, 2, 1, 2)
        ctrl = ControlMessageSerializer.deserialize(payload)
        assert ctrl.expected_total == 2
    finally:
        _close_worker(worker)


def test_q3_averages_per_payment_format(monkeypatch, tmp_path):
    module = _aggregator_module(monkeypatch, tmp_path, C_Q3)
    worker, sent = _worker_with_captured_outputs(module)
    try:
        partials = [
            Q3PaymentFormatPartial(payment_format="Wire", amount_sum=100.0, count=4),
            Q3PaymentFormatPartial(payment_format="Wire", amount_sum=50.0, count=1),
            Q3PaymentFormatPartial(payment_format="ACH", amount_sum=30.0, count=3),
        ]
        _run_data_then_eof(
            worker,
            client_id=3,
            payloads=[Q3PaymentFormatPartialSerializer.serialize(p) for p in partials],
        )

        assert [destination for destination, _ in sent] == ["test_output_queue"] * 3
        data_packets = [packet for _, packet in sent[:-1]]
        eof_packet = sent[-1][1]

        avg_by_format = {}
        for packet in data_packets:
            msg_type, client_id, sender_id, _seq, payload = _unpack_output(worker, packet)
            assert (msg_type, client_id, sender_id) == (MessageType.DATA, 3, 1)
            result = Q3AverageResultSerializer.deserialize(payload)
            avg_by_format[result.payment_format] = result.average

        assert avg_by_format == {"Wire": 150.0 / 5, "ACH": 30.0 / 3}

        msg_type, client_id, sender_id, seq, payload = _unpack_output(worker, eof_packet)
        assert (msg_type, client_id, sender_id, seq) == (MessageType.EOF, 3, 1, 2)
        ctrl = ControlMessageSerializer.deserialize(payload)
        assert ctrl.expected_total == 2
    finally:
        _close_worker(worker)
