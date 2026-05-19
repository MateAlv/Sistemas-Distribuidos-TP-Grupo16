import os

from common.constants import C_Q2, C_Q3, C_Q5
from common.domain.partial_result import Q2BankMaxPartial, Q3PaymentFormatPartial
from common.message_protocol.aggregation_serializer import AggregationSerializer
from common.message_protocol.partial_result_serializer import (
    Q2BankMaxPartialSerializer,
    Q3AverageResultSerializer,
    Q3PaymentFormatPartialSerializer,
)
from common import middleware as _middleware


os.environ.setdefault("ID", "1")
os.environ.setdefault("MOM_HOST", "localhost")
os.environ.setdefault("CONFIGURATION", C_Q5)
os.environ.setdefault("AGGREGATION_PREFIX", "test_aggregation")
os.environ.setdefault("OUTPUT_QUEUE", "test_output_queue")


# Replace real RabbitMQ middleware with a dummy that doesn't connect
class _DummyMiddlewareQueue:
    def __init__(self, host, queue_name):
        self._host = host
        self._queue_name = queue_name
    def start_consuming(self, cb):
        pass
    def send(self, msg):
        pass
    def stop_consuming(self):
        pass
    def close(self):
        pass


class _DummyMiddlewareExchange:
    def __init__(self, host, exchange_name, routing_keys, exchange_type="direct", queue_name=None, exclusive=True):
        self._host = host
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
    def start_consuming(self, cb):
        pass
    def send(self, msg):
        pass
    def stop_consuming(self):
        pass
    def close(self):
        pass

_middleware.MessageMiddlewareQueueRabbitMQ = _DummyMiddlewareQueue
_middleware.MessageMiddlewareExchangeRabbitMQ = _DummyMiddlewareExchange

from workers.aggregator.aggregators import AggregatorWorker
from common.message_protocol.control_message_serializer import ControlMessageSerializer
from common.message_protocol.common import MessageType


class DummyQueue:
    def __init__(self):
        self.sent = []

    def send(self, packet: bytes):
        self.sent.append(packet)


def _send_data(worker, client_id: int, payload: bytes) -> None:
    packet = worker.internal_protocol.create_packet(
        msg_type=MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=payload,
    )
    worker._process_data_message(packet)


def _send_eof(worker, client_id: int) -> None:
    packet = worker.internal_protocol.create_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=b"",
    )
    worker._process_data_message(packet)


def test_q5_counts_each_data_message():
    import workers.aggregator.aggregators as ag

    ag.CONFIGURATION = C_Q5

    worker = AggregatorWorker()
    out = DummyQueue()
    worker.output_queue = out

    client_id = 1
    # Q5 has no Sum: Filter Q5 USD<1 forwards each filtered Transaction.
    # The payload content is irrelevant; every DATA counts as one.
    for _ in range(5):
        _send_data(worker, client_id, b"any-transaction-payload")
    _send_eof(worker, client_id)

    # Expect a DATA message with the partial total (5) followed by an EOF.
    assert len(out.sent) == 2
    data_packet, eof_packet = out.sent

    m_type, _, payload = worker.internal_protocol.unpack_packet(data_packet)
    assert m_type == MessageType.DATA
    assert AggregationSerializer.deserialize(payload) == 5

    m_type, _, payload = worker.internal_protocol.unpack_packet(eof_packet)
    assert m_type == MessageType.EOF
    ctrl = ControlMessageSerializer().deserialize(payload)
    assert ctrl.expected_total == 1


def test_duplicate_eof_and_late_data_are_ignored():
    import workers.aggregator.aggregators as ag

    ag.CONFIGURATION = C_Q5

    worker = AggregatorWorker()
    out = DummyQueue()
    worker.output_queue = out

    client_id = 7
    for _ in range(3):
        _send_data(worker, client_id, b"tx")
    _send_eof(worker, client_id)

    # First EOF emits exactly: 1 DATA (count=3) + 1 EOF.
    assert len(out.sent) == 2

    # Duplicate EOF and late DATA for a closed client must be ignored.
    _send_eof(worker, client_id)
    _send_data(worker, client_id, b"late-tx")
    _send_eof(worker, client_id)

    assert len(out.sent) == 2


def test_q2_keeps_max_per_bank():
    import workers.aggregator.aggregators as ag

    ag.CONFIGURATION = C_Q2

    worker = AggregatorWorker()
    out = DummyQueue()
    worker.output_queue = out

    client_id = 2
    # Partials arriving (possibly out of order) from the Sum workers.
    partials = [
        Q2BankMaxPartial(bank_id="X", from_account="a1", amount=10.0),
        Q2BankMaxPartial(bank_id="X", from_account="a2", amount=30.0),
        Q2BankMaxPartial(bank_id="Z", from_account="a3", amount=20.0),
    ]
    for partial in partials:
        _send_data(worker, client_id, Q2BankMaxPartialSerializer.serialize(partial))
    _send_eof(worker, client_id)

    sent = out.sent
    assert len(sent) == 3  # 2 bank maxima + EOF

    data_packets = sent[:-1]
    eof_packet = sent[-1]

    max_by_bank = {}
    for packet in data_packets:
        m_type, _, payload = worker.internal_protocol.unpack_packet(packet)
        assert m_type == MessageType.DATA
        result = Q2BankMaxPartialSerializer.deserialize(payload)
        max_by_bank[result.bank_id] = result.amount

    assert max_by_bank["X"] == 30.0
    assert max_by_bank["Z"] == 20.0

    m_type, _, payload = worker.internal_protocol.unpack_packet(eof_packet)
    assert m_type == MessageType.EOF
    ctrl = ControlMessageSerializer().deserialize(payload)
    assert ctrl.expected_total == 2


def test_q3_averages_per_payment_format():
    import workers.aggregator.aggregators as ag

    ag.CONFIGURATION = C_Q3

    worker = AggregatorWorker()
    out = DummyQueue()
    worker.output_queue = out

    client_id = 3
    # Two Sum workers each contribute partials for the same payment format.
    partials = [
        Q3PaymentFormatPartial(payment_format="Wire", amount_sum=100.0, count=4),
        Q3PaymentFormatPartial(payment_format="Wire", amount_sum=50.0, count=1),
        Q3PaymentFormatPartial(payment_format="ACH", amount_sum=30.0, count=3),
    ]
    for partial in partials:
        _send_data(
            worker, client_id, Q3PaymentFormatPartialSerializer.serialize(partial)
        )
    _send_eof(worker, client_id)

    sent = out.sent
    assert len(sent) == 3  # 2 payment formats + EOF

    data_packets = sent[:-1]
    eof_packet = sent[-1]

    avg_by_format = {}
    for packet in data_packets:
        m_type, _, payload = worker.internal_protocol.unpack_packet(packet)
        assert m_type == MessageType.DATA
        result = Q3AverageResultSerializer.deserialize(payload)
        avg_by_format[result.payment_format] = result.average

    assert avg_by_format["Wire"] == 150.0 / 5
    assert avg_by_format["ACH"] == 30.0 / 3

    m_type, _, payload = worker.internal_protocol.unpack_packet(eof_packet)
    assert m_type == MessageType.EOF
    ctrl = ControlMessageSerializer().deserialize(payload)
    assert ctrl.expected_total == 2
