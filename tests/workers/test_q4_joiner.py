import importlib
import sys

from common.message_protocol.internal import InternalProtocol, Q4BlockJoinEdge
from common.message_protocol.internal.common import MessageType


class FakeExchange:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.closed = False
        self.stop_requested = False

    def start_consuming(self, callback):
        self.callback = callback

    def request_stop_consuming(self):
        self.stop_requested = True

    def close(self):
        self.closed = True


class RecordingPublisher:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.sent = []

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
        self.requeues = []

    def ack(self):
        self.acks += 1

    def nack(self, requeue=False):
        self.nacks += 1
        self.requeues.append(requeue)


def _load_module(
    monkeypatch,
    tmp_path,
    *,
    worker_id=0,
    sum_amount=1,
    pair_partitions=4,
):
    monkeypatch.setenv("ID", str(worker_id))
    monkeypatch.setenv("MOM_HOST", "mom")
    monkeypatch.setenv("Q4_JOINER_EXCHANGE", "q4_joiner")
    monkeypatch.setenv("Q4_JOINER_ROUTING_PREFIX", "q4_joiner")
    monkeypatch.setenv("Q4_SUM_AMOUNT", str(sum_amount))
    monkeypatch.setenv("Q4_AGGREGATOR_EXCHANGE", "q4_aggregator")
    monkeypatch.setenv("Q4_AGGREGATOR_AMOUNT", str(pair_partitions))
    monkeypatch.setenv("Q4_AGGREGATOR_ROUTING_PREFIX", "q4_aggregator")
    monkeypatch.setenv("Q4_JOINER_BATCH_MAX_DELTAS", "5000")
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    monkeypatch.setenv("SNAPSHOT_INTERVAL", "1000")

    module_name = "workers.scatter_gather.q4_joiner.joiner"
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)

    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    monkeypatch.setattr(module, "ShardedPublisher", RecordingPublisher)
    return module


def _worker(module):
    return module.Q4JoinerWorker()


def _account(module, bank, account):
    return module.Q4AccountId(bank_id=bank, account=account)


def _block_edge(module, role, intermediate, endpoint, count, a_bucket=0, b_bucket=0):
    return Q4BlockJoinEdge(
        role=role,
        intermediate=intermediate,
        endpoint=endpoint,
        a_bucket=a_bucket,
        b_bucket=b_bucket,
        count=count,
    )


def _data_packet(module, client_id, edges, sender_id=0, seq=0):
    return InternalProtocol.create_addressed_packet(
        MessageType.DATA,
        client_id.to_bytes(16, "big"),
        sender_id,
        seq,
        module.Q4BlockJoinEdgeSerializer.serialize_batch(edges),
    )


def _eof_packet(worker, client_id, upstream_id, expected_total=0, seq=0):
    return InternalProtocol.create_addressed_packet(
        MessageType.EOF,
        client_id.to_bytes(16, "big"),
        upstream_id,
        seq,
        worker._control_payload(upstream_id, expected_total, 0),
    )


def _pair_data_messages(worker, module):
    deltas = []
    by_partition = {}
    for shard, packet in worker._pair_reducer_output.sent:
        msg_type, _, sender_id, _seq, payload = (
            worker._proto.unpack_addressed_packet(packet)
        )
        assert sender_id == module.ID
        if msg_type != MessageType.DATA:
            continue
        batch = module.Q4PairPathsSerializer.deserialize_batch(payload)
        deltas.extend(batch)
        by_partition[shard] = by_partition.get(shard, 0) + len(batch)
    return deltas, by_partition


def _eof_counts(worker, module):
    counts = {}
    for shard, packet in worker._pair_reducer_output.sent:
        msg_type, _, sender_id, _seq, payload = (
            worker._proto.unpack_addressed_packet(packet)
        )
        assert sender_id == module.ID
        if msg_type != MessageType.EOF:
            continue
        control = worker._control_serializer.deserialize(payload)
        assert control.sender_id == module.ID
        counts[shard] = control.expected_total
    return counts


def test_q4_joiner_predeclares_aggregator_bindings(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, pair_partitions=3)
    calls = []
    monkeypatch.setattr(
        module,
        "ensure_exchange_queue_bindings",
        lambda *args: calls.append(args),
    )

    _worker(module)._ensure_output_bindings()

    assert calls == [
        (
            "mom",
            "q4_aggregator",
            {
                "q4_aggregator_0": "q4_aggregator_0",
                "q4_aggregator_1": "q4_aggregator_1",
                "q4_aggregator_2": "q4_aggregator_2",
            },
        )
    ]


def test_data_accumulates_and_dedups_redelivery(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_amount=1)
    worker = _worker(module)
    calls = AckNack()
    client_id = 50
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    packet = _data_packet(
        module,
        client_id,
        [_block_edge(module, module.Q4_EDGE_INCOMING, m, a, 3)],
        sender_id=4,
        seq=0,
    )

    worker._on_message(packet, calls.ack, calls.nack)
    worker._on_message(packet, calls.ack, calls.nack)

    assert calls.acks == 2
    assert calls.nacks == 0
    assert worker._state.processed_count(client_id) == 1
    assert worker._state.incoming_for(client_id)[(m, 0, 0)][a] == 3
    assert worker._pair_reducer_output.sent == []


def test_joiner_waits_for_all_eofs_and_emits_weighted_pair_paths(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_amount=2, pair_partitions=5)
    worker = _worker(module)
    calls = AckNack()
    client_id = 51
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")
    c = _account(module, "4", "C")
    d = _account(module, "5", "D")

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [
                _block_edge(module, module.Q4_EDGE_INCOMING, m, a, 3),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, b, 4),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, a, 5),
                _block_edge(module, module.Q4_EDGE_INCOMING, m, c, 2, a_bucket=1, b_bucket=1),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, d, 2, a_bucket=1, b_bucket=1),
            ],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=5, seq=1),
        calls.ack,
        calls.nack,
    )
    assert worker._pair_reducer_output.sent == []

    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=1, expected_total=0, seq=0),
        calls.ack,
        calls.nack,
    )

    deltas, by_partition = _pair_data_messages(worker, module)
    assert {(delta.source, delta.target, delta.path_count) for delta in deltas} == {
        (a, b, module.Q4_QUALIFY_THRESHOLD),
        (c, d, 4),
    }
    assert (a, a, module.Q4_QUALIFY_THRESHOLD) not in {
        (delta.source, delta.target, delta.path_count) for delta in deltas
    }
    for shard, packet in worker._pair_reducer_output.sent:
        msg_type, _, sender_id, _seq, payload = (
            worker._proto.unpack_addressed_packet(packet)
        )
        assert sender_id == module.ID
        if msg_type != MessageType.DATA:
            continue
        for delta in module.Q4PairPathsSerializer.deserialize_batch(payload):
            assert shard == worker._pair_partition(delta.source, delta.target)
    assert _eof_counts(worker, module) == {
        partition: by_partition.get(partition, 0) for partition in range(5)
    }
    assert worker._state.is_closed(client_id)
    assert calls.nacks == 0


def test_joiner_does_not_join_edges_from_different_blocks(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_amount=1, pair_partitions=3)
    worker = _worker(module)
    calls = AckNack()
    client_id = 52
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [
                _block_edge(module, module.Q4_EDGE_INCOMING, m, a, 1, a_bucket=0, b_bucket=0),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, b, 1, a_bucket=0, b_bucket=1),
            ],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=2, seq=1),
        calls.ack,
        calls.nack,
    )

    assert _pair_data_messages(worker, module)[0] == []
    assert _eof_counts(worker, module) == {0: 0, 1: 0, 2: 0}


def test_duplicate_eof_and_late_data_after_close_do_not_reemit(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_amount=1, pair_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 53
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [
                _block_edge(module, module.Q4_EDGE_INCOMING, m, a, 1),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, b, 1),
            ],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    eof = _eof_packet(worker, client_id, upstream_id=0, expected_total=2, seq=1)
    worker._on_message(eof, calls.ack, calls.nack)
    emitted_after_close = len(worker._pair_reducer_output.sent)

    worker._on_message(eof, calls.ack, calls.nack)
    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_block_edge(module, module.Q4_EDGE_OUTGOING, m, _account(module, "9", "LATE"), 1)],
            sender_id=0,
            seq=2,
        ),
        calls.ack,
        calls.nack,
    )

    assert len(worker._pair_reducer_output.sent) == emitted_after_close
    assert calls.nacks == 0


def test_recovery_restores_accumulated_counts(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_amount=2)
    worker = _worker(module)
    calls = AckNack()
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    worker._on_message(
        _data_packet(
            module,
            54,
            [_block_edge(module, module.Q4_EDGE_INCOMING, m, a, 4)],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )

    recovered = _worker(module)
    assert recovered._state.processed_count(54) == 1
    assert recovered._state.incoming_for(54)[(m, 0, 0)][a] == 4


def test_recovery_republishes_data_outbox_after_crash_before_publish(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_amount=1, pair_partitions=3)
    worker = _worker(module)
    calls = AckNack()
    client_id = 55
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")
    worker._on_message(
        _data_packet(
            module,
            client_id,
            [
                _block_edge(module, module.Q4_EDGE_INCOMING, m, a, 2),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, b, 3),
            ],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )

    def crash_publish(_entries):
        raise RuntimeError("crash after EOF INPUT_APPLIED")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=2, seq=1),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1

    recovered = _worker(module)
    deltas, _ = _pair_data_messages(recovered, module)
    assert [(d.source, d.target, d.path_count) for d in deltas] == [(a, b, 6)]
    assert set(_eof_counts(recovered, module)) == {0, 1, 2}
    assert recovered._state.is_closed(client_id)


def test_recovery_republishes_outbox_after_publish_before_done(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_amount=1, pair_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 56
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")
    worker._on_message(
        _data_packet(
            module,
            client_id,
            [
                _block_edge(module, module.Q4_EDGE_INCOMING, m, a, 1),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, b, 1),
            ],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )

    def crash_commit(*_args, **_kwargs):
        raise RuntimeError("crash after publish before INPUT_DONE")

    monkeypatch.setattr(worker._handler, "commit_done", crash_commit)
    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=2, seq=1),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1
    assert len(_pair_data_messages(worker, module)[0]) == 1

    recovered = _worker(module)
    assert len(_pair_data_messages(recovered, module)[0]) == 1
    assert recovered._state.is_closed(client_id)


def test_recovery_after_done_before_ack_does_not_reapply_or_republish(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_amount=1, pair_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 57
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    packet = _data_packet(
        module,
        client_id,
        [_block_edge(module, module.Q4_EDGE_INCOMING, m, a, 4)],
        sender_id=0,
        seq=0,
    )

    def crash_ack():
        raise RuntimeError("crash after INPUT_DONE before ack")

    worker._on_message(packet, crash_ack, calls.nack)
    assert calls.nacks == 1

    recovered = _worker(module)
    assert recovered._pair_reducer_output.sent == []
    redelivery = AckNack()
    recovered._on_message(packet, redelivery.ack, redelivery.nack)
    assert redelivery.acks == 1
    assert redelivery.nacks == 0
    assert recovered._state.processed_count(client_id) == 1
    assert recovered._pair_reducer_output.sent == []
