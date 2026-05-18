import os

from common.constants import C_Q2, C_Q5
from common.domain.transaction import Transaction
from common import middleware as _middleware


os.environ.setdefault("ID", "1")
os.environ.setdefault("MOM_HOST", "localhost")
os.environ.setdefault("CONFIGURATION", C_Q5)
os.environ.setdefault("INPUT_QUEUE", "test_input")
os.environ.setdefault("DATA_QUEUE", "test_data")
os.environ.setdefault("SUM_Q2_EXCHANGE", "test_sum_q2_exchange")
os.environ.setdefault("AGG_Q2_OUTPUT_QUEUE", "test_agg_q2_output")
os.environ.setdefault("JOIN_Q3_EXCHANGE", "test_join_q3_exchange")
os.environ.setdefault("AGG_Q3_DATA_QUEUE", "test_agg_q3_data")
os.environ.setdefault("AGG_Q3_OUTPUT_QUEUE", "test_agg_q3_output")


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


def test_q5_streaming_counts():
    # Ensure module is in Q5 mode
    import workers.aggregator.aggregators as ag

    ag.CONFIGURATION = C_Q5

    worker = AggregatorWorker()
    out = DummyQueue()
    worker.output_queues[ag.DATA_QUEUE] = out

    client_id = 1
    for i in range(1, 4):
        tx = Transaction(
            "2020-01-01",
            "A",
            "acctA",
            "B",
            "acctB",
            i * 10,
            "USD",
            "LI-Mini",
        )
        packet = worker.internal_packet_serializer.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=worker.transaction_serializer.serialize(tx),
        )
        worker._process_data_message(packet)
    # Now send EOF and expect a single EOF with the final count (3)
    eof_packet_in = worker.internal_packet_serializer.create_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=b"",
    )
    worker._process_data_message(eof_packet_in)

    # last sent packet should be EOF with aggregated count == 3
    last = out.sent[-1]
    msg_type, cid, payload = worker.internal_packet_serializer.unpack_packet(last)
    assert msg_type == MessageType.EOF
    val = worker.aggregation_serializer.deserialize(payload)
    assert val == 3


def test_q2_eof_expected_total_and_maxima():
    import workers.aggregator.aggregators as ag

    ag.CONFIGURATION = C_Q2

    worker = AggregatorWorker()
    out = DummyQueue()
    worker.output_queues[ag.AGG_Q2_OUTPUT_QUEUE] = out

    client_id = 2
    # send multiple transactions for two banks
    tx1 = Transaction("2020-01-01", "X", "a1", "Y", "b1", 10, "USD", "LI-Mini")
    tx2 = Transaction("2020-01-01", "X", "a2", "Y", "b2", 30, "USD", "LI-Mini")
    tx3 = Transaction("2020-01-01", "Z", "a3", "Y", "b3", 20, "USD", "LI-Mini")

    for tx in (tx1, tx2, tx3):
        packet = worker.internal_packet_serializer.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=worker.transaction_serializer.serialize(tx),
        )
        worker._process_data_message(packet)

    # send EOF
    eof_packet_in = worker.internal_packet_serializer.create_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=b"",
    )
    worker._process_data_message(eof_packet_in)

    # output_queue should have DATA packets for each bank max (2 banks) and an EOF
    sent = out.sent
    assert len(sent) == 3

    data_packets = sent[:-1]
    eof_packet = sent[-1]

    max_by_bank = {}
    for packet in data_packets:
        m_type, _, tx_payload = worker.internal_packet_serializer.unpack_packet(packet)
        assert m_type == MessageType.DATA
        tx = worker.transaction_serializer.deserialize(tx_payload)
        max_by_bank[tx.from_bank] = tx.amount

    assert max_by_bank["X"] == 30
    assert max_by_bank["Z"] == 20

    # last packet should be EOF
    msg_type, cid, payload = worker.internal_packet_serializer.unpack_packet(eof_packet)
    assert msg_type == MessageType.EOF

    ctrl = ControlMessageSerializer().deserialize(payload)
    # expected_total should equal number of DATA messages sent (2 banks)
    assert ctrl.expected_total == 2
