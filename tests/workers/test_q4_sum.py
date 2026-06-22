import importlib
import sys

from common.message_protocol.internal import Q4CountedEdge, Q4CountedEdgeSerializer
from common.message_protocol.internal.common import MessageType
from common.message_protocol.internal.protocol import InternalProtocol


class FakeExchange:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.stop_requested = False
        self.closed = False

    def start_consuming(self, callback):
        self.callback = callback

    def request_stop_consuming(self):
        self.stop_requested = True

    def close(self):
        self.closed = True


class RecordingPublisher:
    def __init__(self, *args, **kwargs):
        self.sent = []  # list of (shard, body)

    def send(self, body):
        self.sent.append((None, body))

    def send_to_shard(self, body, shard):
        self.sent.append((shard, body))

    def close(self):
        pass


class AckNack:
    def __init__(self):
        self.acks = 0
        self.nacks = 0

    def ack(self):
        self.acks += 1

    def nack(self, requeue=False):
        self.nacks += 1


def _load_module(
    monkeypatch,
    tmp_path,
    *,
    worker_id=0,
    filter_amount=1,
    joiner_amount=3,
    hot_threshold=1_000_000,
    a_buckets=16,
    b_buckets=16,
):
    monkeypatch.setenv("ID", str(worker_id))
    monkeypatch.setenv("MOM_HOST", "mom")
    monkeypatch.setenv("Q4_SUM_EXCHANGE", "q4_sum")
    monkeypatch.setenv("Q4_SUM_ROUTING_PREFIX", "q4_sum")
    monkeypatch.setenv("Q4_FILTER_AMOUNT", str(filter_amount))
    monkeypatch.setenv("Q4_JOINER_EXCHANGE", "q4_joiner")
    monkeypatch.setenv("Q4_JOINER_AMOUNT", str(joiner_amount))
    monkeypatch.setenv("Q4_JOINER_ROUTING_PREFIX", "q4_joiner")
    monkeypatch.setenv("Q4_SUM_BATCH_MAX_EDGES", "5000")
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    monkeypatch.setenv("SNAPSHOT_INTERVAL", "1000")

    module_name = "workers.scatter_gather.q4_sum.sums"
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)

    monkeypatch.setattr(module, "Q4_SUM_HOT_PAIR_THRESHOLD", hot_threshold)
    monkeypatch.setattr(module, "Q4_SUM_HOT_A_BUCKETS", a_buckets)
    monkeypatch.setattr(module, "Q4_SUM_HOT_B_BUCKETS", b_buckets)
    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    monkeypatch.setattr(module, "ShardedPublisher", RecordingPublisher)
    return module


def _worker(module):
    worker = module.Q4SumWorker()
    worker._runner.recover_and_republish()
    return worker


def _account(module, bank, account):
    return module.Q4AccountId(bank_id=bank, account=account)


def _counted(module, role, intermediate, endpoint, count=1):
    return Q4CountedEdge(role=role, intermediate=intermediate, endpoint=endpoint, count=count)


def _data_packet(module, client_id, edges, sender_id=0, seq=0):
    return InternalProtocol.create_addressed_packet(
        MessageType.DATA,
        client_id.to_bytes(16, "big"),
        sender_id,
        seq,
        Q4CountedEdgeSerializer.serialize_batch(edges),
    )


def _eof_packet(worker, client_id, upstream_id, expected_total=0, seq=0):
    return InternalProtocol.create_addressed_packet(
        MessageType.EOF,
        client_id.to_bytes(16, "big"),
        upstream_id,
        seq,
        worker._control_payload(upstream_id, expected_total, 0),
    )


def _block_messages(worker, module):
    edges, by_partition = [], {}
    for shard, body in worker._joiner_output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(body)
        if msg_type != MessageType.DATA:
            continue
        batch = module.Q4BlockJoinEdgeSerializer.deserialize_batch(payload)
        edges.extend(batch)
        by_partition[shard] = by_partition.get(shard, 0) + len(batch)
    return edges, by_partition


def _eof_counts(worker):
    counts = {}
    for shard, body in worker._joiner_output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(body)
        if msg_type != MessageType.EOF:
            continue
        counts[shard] = worker._control_serializer.deserialize(payload).expected_total
    return counts


def test_q4_sum_predeclares_joiner_bindings(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, joiner_amount=3)
    calls = []
    monkeypatch.setattr(
        module, "ensure_exchange_queue_bindings", lambda *args: calls.append(args)
    )

    module.Q4SumWorker()._ensure_output_bindings()

    assert calls == [
        (
            "mom",
            "q4_joiner",
            {
                "q4_joiner_0": "q4_joiner_0",
                "q4_joiner_1": "q4_joiner_1",
                "q4_joiner_2": "q4_joiner_2",
            },
        )
    ]


def test_data_accumulates_and_dedups_redelivery(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path)
    worker = _worker(module)
    calls = AckNack()
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    packet = _data_packet(
        module, 7, [_counted(module, module.Q4_EDGE_INCOMING, m, a, 3)], sender_id=0, seq=0
    )

    worker._on_message(packet, calls.ack, calls.nack)
    worker._on_message(packet, calls.ack, calls.nack)  # redelivery, same (sender, seq)

    assert calls.acks == 2
    assert calls.nacks == 0
    # Applied once: count not doubled, nothing emitted on the data path.
    assert worker._state.processed_count(7) == 1
    assert worker._joiner_output.sent == []


def test_fan_in_waits_for_all_upstream_eofs_then_emits(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, filter_amount=2, joiner_amount=4)
    worker = _worker(module)
    calls = AckNack()
    client_id = 41
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")
    no_join = _account(module, "9", "NO_JOIN")

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [
                _counted(module, module.Q4_EDGE_INCOMING, m, a, 2),
                _counted(module, module.Q4_EDGE_INCOMING, m, a, 3),
                _counted(module, module.Q4_EDGE_OUTGOING, m, b, 7),
                _counted(module, module.Q4_EDGE_INCOMING, no_join, a, 11),
            ],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )

    worker._on_message(_eof_packet(worker, client_id, upstream_id=0, expected_total=4, seq=1), calls.ack, calls.nack)
    assert worker._joiner_output.sent == []  # only one of two upstream EOFs

    worker._on_message(_eof_packet(worker, client_id, upstream_id=1, expected_total=0, seq=0), calls.ack, calls.nack)

    edges, by_partition = _block_messages(worker, module)
    assert {edge.intermediate for edge in edges} == {m}
    assert {
        (edge.role, edge.endpoint, edge.a_bucket, edge.b_bucket, edge.count) for edge in edges
    } == {
        (module.Q4_EDGE_INCOMING, a, 0, 0, 5),
        (module.Q4_EDGE_OUTGOING, b, 0, 0, 7),
    }
    # One EOF per joiner partition, each carrying that partition's record count.
    assert _eof_counts(worker) == {
        partition: by_partition.get(partition, 0) for partition in range(4)
    }
    assert sum(by_partition.values()) == 2
    assert worker._state.is_closed(client_id)
    assert calls.nacks == 0


def test_hot_intermediary_fans_out_to_blocks(monkeypatch, tmp_path):
    module = _load_module(
        monkeypatch, tmp_path, filter_amount=1, joiner_amount=5,
        hot_threshold=1, a_buckets=2, b_buckets=3,
    )
    worker = _worker(module)
    calls = AckNack()
    client_id = 42
    m = _account(module, "2", "HOT")
    a1 = _account(module, "1", "A1")
    a2 = _account(module, "1", "A2")
    b1 = _account(module, "3", "B1")
    b2 = _account(module, "3", "B2")

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [
                _counted(module, module.Q4_EDGE_INCOMING, m, a1, 2),
                _counted(module, module.Q4_EDGE_INCOMING, m, a2, 4),
                _counted(module, module.Q4_EDGE_OUTGOING, m, b1, 3),
                _counted(module, module.Q4_EDGE_OUTGOING, m, b2, 5),
            ],
        ),
        calls.ack,
        calls.nack,
    )
    worker._on_message(_eof_packet(worker, client_id, upstream_id=0, expected_total=4, seq=1), calls.ack, calls.nack)

    edges, by_partition = _block_messages(worker, module)
    incoming = [e for e in edges if e.role == module.Q4_EDGE_INCOMING]
    outgoing = [e for e in edges if e.role == module.Q4_EDGE_OUTGOING]
    assert len(incoming) == 6  # 2 endpoints x 3 b_buckets
    assert len(outgoing) == 4  # 2 endpoints x 2 a_buckets

    for shard, body in worker._joiner_output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(body)
        if msg_type != MessageType.DATA:
            continue
        for edge in module.Q4BlockJoinEdgeSerializer.deserialize_batch(payload):
            assert shard == worker._block_partition(edge.intermediate, edge.a_bucket, edge.b_bucket)

    assert sum(by_partition.values()) == 10
    assert sum(_eof_counts(worker).values()) == 10


def test_duplicate_eof_and_late_data_after_close_do_not_reemit(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, filter_amount=1, joiner_amount=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 43
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [
                _counted(module, module.Q4_EDGE_INCOMING, m, a, 1),
                _counted(module, module.Q4_EDGE_OUTGOING, m, b, 1),
            ],
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    eof = _eof_packet(worker, client_id, upstream_id=0, expected_total=2, seq=1)
    worker._on_message(eof, calls.ack, calls.nack)
    emitted = len(worker._joiner_output.sent)
    assert emitted > 0

    worker._on_message(eof, calls.ack, calls.nack)  # duplicate EOF (deduped)
    worker._on_message(
        _data_packet(
            module, client_id,
            [_counted(module, module.Q4_EDGE_INCOMING, m, _account(module, "4", "X"))],
            seq=2,
        ),
        calls.ack, calls.nack,
    )  # late data for a closed client

    assert len(worker._joiner_output.sent) == emitted
    assert calls.nacks == 0


def test_recovery_restores_accumulated_counts(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, filter_amount=2)
    worker = _worker(module)
    calls = AckNack()
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    worker._on_message(
        _data_packet(module, 7, [_counted(module, module.Q4_EDGE_INCOMING, m, a, 4)], seq=0),
        calls.ack, calls.nack,
    )
    assert worker._state.processed_count(7) == 1

    # Simulate a restart: a fresh worker over the same STATE_DIR recovers state.
    recovered = _worker(module)
    assert recovered._state.processed_count(7) == 1
    assert recovered._state.incoming_for(7)[m][a] == 4
