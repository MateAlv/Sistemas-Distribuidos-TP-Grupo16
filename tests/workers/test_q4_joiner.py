import importlib
import sys

from common.message_protocol.internal import Q4BlockJoinEdge
from common.message_protocol.internal.common import MessageType


def _load_module(
    monkeypatch,
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
    monkeypatch.setenv("Q4_JOINER_BATCH_BYTES", str(1024 * 1024))
    monkeypatch.setenv("Q4_JOINER_BATCH_MAX_DELTAS", "5000")

    module_name = "workers.scatter_gather.q4_joiner.joiner"
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)

    class FakeExchange:
        instances = []

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.sent = []
            self.closed = False
            self.stop_requested = False
            FakeExchange.instances.append(self)

        def send(self, message, routing_key=None):
            self.sent.append((routing_key, message))

        def start_consuming(self, callback):
            self.callback = callback

        def request_stop_consuming(self):
            self.stop_requested = True

        def close(self):
            self.closed = True

    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    return module, FakeExchange


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


def _data_payload(module, edges):
    return module.Q4BlockJoinEdgeSerializer.serialize_batch(edges)


def _control_payload(worker, sender_id, expected_total, processed_count=0):
    return worker._control_payload(sender_id, expected_total, processed_count)


def _pair_data_messages(worker, module, output):
    deltas = []
    by_partition = {}
    for routing_key, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        batch = module.Q4PairPathsSerializer.deserialize_batch(payload)
        deltas.extend(batch)
        by_partition[routing_key] = by_partition.get(routing_key, 0) + len(batch)
    return deltas, by_partition


def _eof_counts(worker, output):
    counts = {}
    for routing_key, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.EOF:
            continue
        control = worker._control_serializer.deserialize(payload)
        counts[routing_key] = control.expected_total
    return counts


def test_q4_joiner_predeclares_aggregator_bindings(monkeypatch):
    module, _ = _load_module(monkeypatch, pair_partitions=3)
    calls = []
    monkeypatch.setattr(
        module,
        "ensure_exchange_queue_bindings",
        lambda *args: calls.append(args),
    )

    module.Q4JoinerWorker()._ensure_output_bindings()

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


def test_block_joiner_waits_for_all_eofs_and_emits_weighted_pair_pathss(
    monkeypatch,
):
    module, _ = _load_module(monkeypatch, sum_amount=2, pair_partitions=5)
    worker = module.Q4JoinerWorker()
    output = worker._pair_reducer_output
    client_id = 51

    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")
    c = _account(module, "4", "C")
    d = _account(module, "5", "D")

    worker._accept_block_edges(
        client_id,
        _data_payload(
            module,
            [
                _block_edge(module, module.Q4_EDGE_INCOMING, m, a, 3),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, b, 4),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, a, 5),
                _block_edge(
                    module,
                    module.Q4_EDGE_INCOMING,
                    m,
                    c,
                    2,
                    a_bucket=1,
                    b_bucket=1,
                ),
                _block_edge(
                    module,
                    module.Q4_EDGE_OUTGOING,
                    m,
                    d,
                    2,
                    a_bucket=1,
                    b_bucket=1,
                ),
            ],
        ),
    )

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=5),
    )
    assert output.sent == []

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=1, expected_total=0),
    )

    deltas, by_partition = _pair_data_messages(worker, module, output)
    assert {
        (delta.source, delta.target, delta.path_count)
        for delta in deltas
    } == {
        (a, b, module.Q4_QUALIFY_THRESHOLD),
        (c, d, 4),
    }
    assert (a, a, module.Q4_QUALIFY_THRESHOLD) not in {
        (delta.source, delta.target, delta.path_count)
        for delta in deltas
    }

    for routing_key, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        for delta in module.Q4PairPathsSerializer.deserialize_batch(payload):
            expected_partition = worker._pair_partition(delta.source, delta.target)
            assert routing_key == f"q4_aggregator_{expected_partition}"

    assert _eof_counts(worker, output) == {
        f"q4_aggregator_{partition}": by_partition.get(
            f"q4_aggregator_{partition}", 0
        )
        for partition in range(5)
    }
    assert client_id in worker._closed_by_client


def test_block_joiner_does_not_join_edges_from_different_blocks(monkeypatch):
    module, _ = _load_module(monkeypatch, sum_amount=1, pair_partitions=3)
    worker = module.Q4JoinerWorker()
    output = worker._pair_reducer_output
    client_id = 52

    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")

    worker._accept_block_edges(
        client_id,
        _data_payload(
            module,
            [
                _block_edge(
                    module,
                    module.Q4_EDGE_INCOMING,
                    m,
                    a,
                    1,
                    a_bucket=0,
                    b_bucket=0,
                ),
                _block_edge(
                    module,
                    module.Q4_EDGE_OUTGOING,
                    m,
                    b,
                    1,
                    a_bucket=0,
                    b_bucket=1,
                ),
            ],
        ),
    )
    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=2),
    )

    assert _pair_data_messages(worker, module, output)[0] == []
    assert _eof_counts(worker, output) == {
        "q4_aggregator_0": 0,
        "q4_aggregator_1": 0,
        "q4_aggregator_2": 0,
    }


def test_block_joiner_duplicate_eof_and_late_data_do_not_reemit(monkeypatch):
    module, _ = _load_module(monkeypatch, sum_amount=1, pair_partitions=2)
    worker = module.Q4JoinerWorker()
    output = worker._pair_reducer_output
    client_id = 53

    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")

    worker._accept_block_edges(
        client_id,
        _data_payload(
            module,
            [
                _block_edge(module, module.Q4_EDGE_INCOMING, m, a, 1),
                _block_edge(module, module.Q4_EDGE_OUTGOING, m, b, 1),
            ],
        ),
    )
    eof_payload = _control_payload(worker, sender_id=0, expected_total=2)
    worker._handle_eof(client_id, eof_payload)
    emitted_after_close = len(output.sent)

    worker._handle_eof(client_id, eof_payload)
    worker._accept_block_edges(
        client_id,
        _data_payload(
            module,
            [
                _block_edge(
                    module,
                    module.Q4_EDGE_OUTGOING,
                    m,
                    _account(module, "9", "LATE"),
                    1,
                )
            ],
        ),
    )

    assert len(output.sent) == emitted_after_close
