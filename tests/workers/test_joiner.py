import os
import tempfile

from common.constants import C_Q2, C_Q5
from common.domain.partial_result import Q2BankMaxPartial
from common.message_protocol.internal.aggregation_serializer import AggregationSerializer
from common.message_protocol.internal.partial_result_serializer import Q2BankMaxPartialSerializer
from common import middleware as _middleware


os.environ.setdefault("ID", "0")
os.environ.setdefault("MOM_HOST", "localhost")
os.environ.setdefault("CONFIGURATION", C_Q2)
os.environ.setdefault("INPUT_QUEUE", "test_join_input")
os.environ.setdefault("OUTPUT_QUEUE", "test_join_output")
os.environ.setdefault("AGGREGATION_AMOUNT", "2")
os.environ.setdefault("Q3_BARRIER_AMOUNT", "1")
os.environ.setdefault("Q3_AVERAGES_EXCHANGE", "q3_averages_exchange")
os.environ.setdefault("Q3_AVERAGES_ROUTING_PREFIX", "q3_averages")


class _DummyQueue:
    def __init__(self, host, queue_name):
        pass
    def start_consuming(self, cb): pass
    def send(self, msg): pass
    def stop_consuming(self): pass
    def close(self): pass


_middleware.MessageMiddlewareQueueRabbitMQ = _DummyQueue

from workers.joiner.joiners import JoinerWorker  # noqa: E402
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer  # noqa: E402
from common.message_protocol.internal.common import MessageType  # noqa: E402


class DummyQueue:
    def __init__(self):
        self.sent = []

    def send(self, packet: bytes):
        self.sent.append(packet)


def _send_data(worker, client_id: int, payload: bytes) -> None:
    packet = worker.internal_protocol.create_addressed_packet(
        msg_type=MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=0,
        seq=worker.data_count_by_client.get(client_id, 0),
        payload=payload,
    )
    worker._process_message(packet)


def _send_eof(worker, client_id: int) -> None:
    packet = worker.internal_protocol.create_addressed_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=0,
        seq=10_000 + worker.eof_count_by_client.get(client_id, 0),
        payload=b"",
    )
    worker._process_message(packet)


def _make_worker(configuration: str, aggregation_amount: int = 2) -> JoinerWorker:
    import workers.joiner.joiners as jm
    jm.CONFIGURATION = configuration
    jm.AGGREGATION_AMOUNT = aggregation_amount
    jm.STATE_DIR = tempfile.mkdtemp(prefix="joiner-test-")
    worker = JoinerWorker()
    worker.output_resource = DummyQueue()
    worker.output_queue = worker.output_resource
    return worker


def test_q3_joiner_predeclares_average_bindings(monkeypatch):
    import workers.joiner.joiners as module

    module.CONFIGURATION = "Q3"
    module.MOM_HOST = "rabbitmq"
    module.INPUT_QUEUE = "join_q3_queue"
    module.OUTPUT_QUEUE = "join_q3_results_queue"
    module.AGGREGATION_AMOUNT = 1
    module.Q3_BARRIER_AMOUNT = 2
    module.Q3_AVERAGES_EXCHANGE = "q3_averages_exchange"
    module.Q3_AVERAGES_ROUTING_PREFIX = "q3_averages"

    calls = []
    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", _DummyQueue)
    monkeypatch.setattr(
        module.middleware,
        "ShardedByClientPublisher",
        lambda *args, **kwargs: DummyQueue(),
    )
    monkeypatch.setattr(
        module,
        "ensure_exchange_queue_bindings",
        lambda *args: calls.append(args),
    )

    module.JoinerWorker()._ensure_output_bindings()

    assert calls == [
        (
            "rabbitmq",
            "q3_averages_exchange",
            {
                "q3_averages_0": "q3_averages_0",
                "q3_averages_1": "q3_averages_1",
            },
        )
    ]


# --- Q5 ---

def test_q5_no_emit_after_first_eof():
    worker = _make_worker(C_Q5, aggregation_amount=2)
    out = worker.output_queue

    _send_data(worker, 1, AggregationSerializer.serialize(3))
    _send_eof(worker, 1)

    assert len(out.sent) == 0, "no debe emitir hasta recibir los 2 EOFs"


def test_q5_emits_after_all_eofs():
    worker = _make_worker(C_Q5, aggregation_amount=2)
    out = worker.output_queue

    _send_data(worker, 1, AggregationSerializer.serialize(3))
    _send_eof(worker, 1)
    _send_data(worker, 1, AggregationSerializer.serialize(7))
    _send_eof(worker, 1)

    assert len(out.sent) == 2  # 1 DATA (count=10) + 1 EOF

    data_packet, eof_packet = out.sent

    m_type, _, payload = worker.internal_protocol.unpack_packet(data_packet)
    assert m_type == MessageType.DATA
    assert AggregationSerializer.deserialize(payload) == 10

    m_type, _, payload = worker.internal_protocol.unpack_packet(eof_packet)
    assert m_type == MessageType.EOF
    ctrl = ControlMessageSerializer().deserialize(payload)
    assert ctrl.expected_total == 1


# --- Q2 ---

def test_q2_reduces_global_max_across_shards():
    worker = _make_worker(C_Q2, aggregation_amount=2)
    out = worker.output_queue

    # shard 0: BANK_X=80, BANK_Y=50
    shard_0 = [
        Q2BankMaxPartial(bank_id="BANK_X", from_account="a1", amount=80.0),
        Q2BankMaxPartial(bank_id="BANK_Y", from_account="a2", amount=50.0),
    ]
    for p in shard_0:
        _send_data(worker, 2, Q2BankMaxPartialSerializer.serialize(p))
    _send_eof(worker, 2)

    assert len(out.sent) == 0  # primer EOF → no emite aún

    # shard 1: BANK_X=100 (supera al shard 0), BANK_Z=30
    shard_1 = [
        Q2BankMaxPartial(bank_id="BANK_X", from_account="a3", amount=100.0),
        Q2BankMaxPartial(bank_id="BANK_Z", from_account="a4", amount=30.0),
    ]
    for p in shard_1:
        _send_data(worker, 2, Q2BankMaxPartialSerializer.serialize(p))
    _send_eof(worker, 2)

    # 3 bancos + 1 EOF
    assert len(out.sent) == 4

    data_packets = out.sent[:-1]
    eof_packet = out.sent[-1]

    max_by_bank = {}
    for packet in data_packets:
        m_type, _, payload = worker.internal_protocol.unpack_packet(packet)
        assert m_type == MessageType.DATA
        r = Q2BankMaxPartialSerializer.deserialize(payload)
        max_by_bank[r.bank_id] = r.amount

    assert max_by_bank["BANK_X"] == 100.0
    assert max_by_bank["BANK_Y"] == 50.0
    assert max_by_bank["BANK_Z"] == 30.0

    m_type, _, payload = worker.internal_protocol.unpack_packet(eof_packet)
    assert m_type == MessageType.EOF
    ctrl = ControlMessageSerializer().deserialize(payload)
    assert ctrl.expected_total == 3


def test_q2_reduces_equal_max_by_earliest_row_across_shards():
    worker = _make_worker(C_Q2, aggregation_amount=2)
    out = worker.output_queue

    _send_data(
        worker,
        4,
        Q2BankMaxPartialSerializer.serialize(
            Q2BankMaxPartial(
                bank_id="BANK_X",
                from_account="later",
                amount=100.0,
                row_number=20,
            )
        ),
    )
    _send_eof(worker, 4)
    _send_data(
        worker,
        4,
        Q2BankMaxPartialSerializer.serialize(
            Q2BankMaxPartial(
                bank_id="BANK_X",
                from_account="earlier",
                amount=100.0,
                row_number=10,
            )
        ),
    )
    _send_eof(worker, 4)

    result = Q2BankMaxPartialSerializer.deserialize(
        worker.internal_protocol.unpack_packet(out.sent[0])[2]
    )
    assert result.from_account == "earlier"
    assert result.row_number == 10


# --- idempotencia ---

def test_duplicate_eof_and_late_data_ignored():
    worker = _make_worker(C_Q5, aggregation_amount=2)
    out = worker.output_queue

    _send_data(worker, 3, AggregationSerializer.serialize(5))
    _send_eof(worker, 3)
    _send_eof(worker, 3)  # segundo EOF → emite

    assert len(out.sent) == 2  # DATA(5) + EOF

    # cualquier mensaje posterior para el mismo cliente debe ignorarse
    _send_eof(worker, 3)
    _send_data(worker, 3, AggregationSerializer.serialize(99))
    _send_eof(worker, 3)

    assert len(out.sent) == 2


# --- múltiples clientes independientes ---

def test_two_clients_are_independent():
    worker = _make_worker(C_Q5, aggregation_amount=2)
    out = worker.output_queue

    # cliente A recibe solo 1 EOF → no emite
    _send_data(worker, 10, AggregationSerializer.serialize(1))
    _send_eof(worker, 10)
    assert len(out.sent) == 0

    # cliente B recibe sus 2 EOFs → emite
    _send_data(worker, 20, AggregationSerializer.serialize(4))
    _send_eof(worker, 20)
    _send_eof(worker, 20)
    assert len(out.sent) == 2  # solo los de cliente B

    # cliente A completa su segundo EOF → emite también
    _send_eof(worker, 10)
    assert len(out.sent) == 4
