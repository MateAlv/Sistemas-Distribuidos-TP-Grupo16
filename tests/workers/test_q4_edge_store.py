import importlib
import sys

from common.message_protocol.internal import Q4CountedEdge
from common.message_protocol.internal.common import MessageType


def _load_module(
    monkeypatch,
    *,
    worker_id=0,
    source_amount=1,
    block_partitions=3,
    hot_threshold=1000000,
    a_buckets=1,
    b_buckets=1,
):
    monkeypatch.setenv("ID", str(worker_id))
    monkeypatch.setenv("MOM_HOST", "mom")
    monkeypatch.setenv("Q4_EDGE_STORE_EXCHANGE", "q4_edge_store")
    monkeypatch.setenv("Q4_EDGE_STORE_ROUTING_PREFIX", "q4_edge_store")
    monkeypatch.setenv("Q4_SOURCE_PREFILTER_AMOUNT", str(source_amount))
    monkeypatch.setenv("Q4_BLOCK_JOINER_EXCHANGE", "q4_block_joiner")
    monkeypatch.setenv("Q4_BLOCK_JOINER_AMOUNT", str(block_partitions))
    monkeypatch.setenv("Q4_BLOCK_JOINER_ROUTING_PREFIX", "q4_block_joiner")
    monkeypatch.setenv("Q4_EDGE_STORE_BATCH_BYTES", str(1024 * 1024))
    monkeypatch.setenv("Q4_EDGE_STORE_BATCH_MAX_EDGES", "5000")
    monkeypatch.setenv("Q4_EDGE_STORE_HOT_PAIR_THRESHOLD", str(hot_threshold))
    monkeypatch.setenv("Q4_EDGE_STORE_HOT_A_BUCKETS", str(a_buckets))
    monkeypatch.setenv("Q4_EDGE_STORE_HOT_B_BUCKETS", str(b_buckets))

    module_name = "workers.q4_edge_store.edge_store"
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


def _counted(module, role, intermediate, endpoint, count=1):
    return Q4CountedEdge(
        role=role,
        intermediate=intermediate,
        endpoint=endpoint,
        count=count,
    )


def _data_payload(module, edges):
    return module.Q4CountedEdgeSerializer.serialize_batch(edges)


def _control_payload(worker, sender_id, expected_total, processed_count=0):
    return worker._control_payload(sender_id, expected_total, processed_count)


def _block_data_messages(worker, module, output):
    edges = []
    by_partition = {}
    for routing_key, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        batch = module.Q4BlockJoinEdgeSerializer.deserialize_batch(payload)
        edges.extend(batch)
        by_partition[routing_key] = by_partition.get(routing_key, 0) + len(batch)
    return edges, by_partition


def _eof_counts(worker, output):
    counts = {}
    for routing_key, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.EOF:
            continue
        control = worker._control_serializer.deserialize(payload)
        counts[routing_key] = control.expected_total
    return counts


def test_edge_store_aggregates_counts_and_waits_for_all_source_eofs(monkeypatch):
    module, _ = _load_module(monkeypatch, source_amount=2, block_partitions=4)
    worker = module.Q4EdgeStoreWorker()
    output = worker._block_joiner_output
    client_id = 41

    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")
    no_join_m = _account(module, "9", "NO_JOIN")

    worker._accept_counted_edges(
        client_id,
        _data_payload(
            module,
            [
                _counted(module, module.Q4_EDGE_INCOMING, m, a, 2),
                _counted(module, module.Q4_EDGE_INCOMING, m, a, 3),
                _counted(module, module.Q4_EDGE_OUTGOING, m, b, 7),
                _counted(module, module.Q4_EDGE_INCOMING, no_join_m, a, 11),
            ],
        ),
    )

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=4),
    )
    assert output.sent == []

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=1, expected_total=0),
    )

    edges, by_partition = _block_data_messages(worker, module, output)
    assert len(edges) == 2
    assert {edge.intermediate for edge in edges} == {m}
    assert {
        (edge.role, edge.endpoint, edge.a_bucket, edge.b_bucket, edge.count)
        for edge in edges
    } == {
        (module.Q4_EDGE_INCOMING, a, 0, 0, 5),
        (module.Q4_EDGE_OUTGOING, b, 0, 0, 7),
    }
    assert _eof_counts(worker, output) == {
        f"q4_block_joiner_{partition}": by_partition.get(
            f"q4_block_joiner_{partition}", 0
        )
        for partition in range(4)
    }
    assert sum(by_partition.values()) == 2
    assert client_id in worker._closed_by_client


def test_edge_store_hot_intermediary_fans_out_to_blocks(monkeypatch):
    module, _ = _load_module(
        monkeypatch,
        source_amount=1,
        block_partitions=5,
        hot_threshold=1,
        a_buckets=2,
        b_buckets=3,
    )
    worker = module.Q4EdgeStoreWorker()
    output = worker._block_joiner_output
    client_id = 42

    m = _account(module, "2", "HOT")
    a1 = _account(module, "1", "A1")
    a2 = _account(module, "1", "A2")
    b1 = _account(module, "3", "B1")
    b2 = _account(module, "3", "B2")

    worker._accept_counted_edges(
        client_id,
        _data_payload(
            module,
            [
                _counted(module, module.Q4_EDGE_INCOMING, m, a1, 2),
                _counted(module, module.Q4_EDGE_INCOMING, m, a2, 4),
                _counted(module, module.Q4_EDGE_OUTGOING, m, b1, 3),
                _counted(module, module.Q4_EDGE_OUTGOING, m, b2, 5),
            ],
        ),
    )
    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=4),
    )

    edges, by_partition = _block_data_messages(worker, module, output)
    incoming = [edge for edge in edges if edge.role == module.Q4_EDGE_INCOMING]
    outgoing = [edge for edge in edges if edge.role == module.Q4_EDGE_OUTGOING]

    assert len(incoming) == 6
    assert len(outgoing) == 4

    for endpoint, count in ((a1, 2), (a2, 4)):
        rows = [edge for edge in incoming if edge.endpoint == endpoint]
        assert len(rows) == 3
        assert {edge.a_bucket for edge in rows} == {
            worker._account_bucket(endpoint, 2)
        }
        assert {edge.b_bucket for edge in rows} == {0, 1, 2}
        assert {edge.count for edge in rows} == {count}

    for endpoint, count in ((b1, 3), (b2, 5)):
        rows = [edge for edge in outgoing if edge.endpoint == endpoint]
        assert len(rows) == 2
        assert {edge.a_bucket for edge in rows} == {0, 1}
        assert {edge.b_bucket for edge in rows} == {
            worker._account_bucket(endpoint, 3)
        }
        assert {edge.count for edge in rows} == {count}

    for routing_key, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        for edge in module.Q4BlockJoinEdgeSerializer.deserialize_batch(payload):
            expected_partition = worker._block_partition(
                edge.intermediate,
                edge.a_bucket,
                edge.b_bucket,
            )
            assert routing_key == f"q4_block_joiner_{expected_partition}"

    assert sum(by_partition.values()) == 10
    assert sum(_eof_counts(worker, output).values()) == 10


def test_edge_store_duplicate_eof_and_late_data_do_not_reemit(monkeypatch):
    module, _ = _load_module(monkeypatch, source_amount=1, block_partitions=2)
    worker = module.Q4EdgeStoreWorker()
    output = worker._block_joiner_output
    client_id = 43
    m = _account(module, "2", "M")
    a = _account(module, "1", "A")
    b = _account(module, "3", "B")

    worker._accept_counted_edges(
        client_id,
        _data_payload(
            module,
            [
                _counted(module, module.Q4_EDGE_INCOMING, m, a, 1),
                _counted(module, module.Q4_EDGE_OUTGOING, m, b, 1),
            ],
        ),
    )
    eof_payload = _control_payload(worker, sender_id=0, expected_total=2)
    worker._handle_eof(client_id, eof_payload)
    emitted_after_close = len(output.sent)

    worker._handle_eof(client_id, eof_payload)
    worker._accept_counted_edges(
        client_id,
        _data_payload(
            module,
            [_counted(module, module.Q4_EDGE_INCOMING, m, _account(module, "4", "X"))],
        ),
    )

    assert len(output.sent) == emitted_after_close
