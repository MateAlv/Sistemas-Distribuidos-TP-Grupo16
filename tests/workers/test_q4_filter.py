import importlib
import sys

from common.domain.transaction import Transaction
from common.message_protocol.internal import Q4CountedEdgeSerializer
from common.message_protocol.internal.common import MessageType


def _load_module(monkeypatch, tmp_path, amount=1, worker_id=0, edge_partitions=3):
    monkeypatch.setenv("ID", str(worker_id))
    monkeypatch.setenv("MOM_HOST", "mom")
    monkeypatch.setenv("INPUT_QUEUE", "q4_filter_input")
    monkeypatch.delenv("Q4_FILTER_INPUT_EXCHANGE", raising=False)
    monkeypatch.setenv("Q4_FILTER_AMOUNT", str(amount))
    monkeypatch.setenv("Q4_FILTER_PREFIX", "q4_filter")
    monkeypatch.setenv("Q4_SUM_EXCHANGE", "q4_sum")
    monkeypatch.setenv("Q4_SUM_AMOUNT", str(edge_partitions))
    monkeypatch.setenv("Q4_SUM_ROUTING_PREFIX", "q4_sum")
    monkeypatch.setenv("Q4_FILTER_BATCH_BYTES", str(1024 * 1024))
    monkeypatch.setenv("Q4_FILTER_BATCH_MAX_EDGES", "5000")
    monkeypatch.setenv("Q4_FILTER_REPLAY_BATCH_EDGES", "3")

    module_name = "workers.scatter_gather.q4_filter.filters"
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)

    class FakeEndpoint:
        instances = []

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.sent = []
            self.closed = False
            self.stop_requested = False
            FakeEndpoint.instances.append(self)

        def send(self, message, routing_key=None):
            self.sent.append((routing_key, message))

        def start_consuming(self, callback):
            self.callback = callback

        def request_stop_consuming(self):
            self.stop_requested = True

        def close(self):
            self.closed = True

    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", FakeEndpoint)
    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", FakeEndpoint)
    return module, FakeEndpoint


def _tx(from_bank="001", from_account="SRC", to_bank="002", to_account="DST"):
    return Transaction(
        date="2022/09/01 00:00",
        from_bank=from_bank,
        from_account=from_account,
        to_bank=to_bank,
        to_account=to_account,
        amount=1.0,
        currency="US Dollar",
        format="ACH",
    )


def _payload(module, transactions):
    return module.TransactionSerializer.serialize_batch(transactions)


def _control_payload(worker, sender_id, expected_total, processed_count=0):
    return worker._control_payload(sender_id, expected_total, processed_count)


def _counted_data_messages(worker, output):
    edges = []
    by_partition = {}
    for routing_key, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        batch = Q4CountedEdgeSerializer.deserialize_batch(payload)
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


def test_source_prefilter_replays_pending_rows_when_source_qualifies(
    monkeypatch, tmp_path
):
    module, _ = _load_module(monkeypatch, tmp_path, amount=1, edge_partitions=4)
    worker = module.Q4FilterWorker()
    output = worker._edge_store_output
    client_id = 17

    not_yet_qualified = [
        _tx(to_bank="002", to_account="M0"),
        _tx(to_bank="002", to_account="M0"),
        _tx(to_bank="003", to_account="M1"),
        _tx(to_bank="004", to_account="M2"),
        _tx(to_bank="005", to_account="M3"),
        _tx(to_bank="006", to_account="M4"),
    ]
    worker._handle_data_packet(client_id, _payload(module, not_yet_qualified))
    worker._flush_client_buffers(client_id)

    assert output.sent == []

    qualifying_row = [_tx(to_bank="007", to_account="M5")]
    worker._handle_data_packet(client_id, _payload(module, qualifying_row))
    worker._flush_client_buffers(client_id)

    edges, by_partition = _counted_data_messages(worker, output)
    assert len(edges) == 14
    assert sum(1 for edge in edges if edge.role == module.Q4_EDGE_INCOMING) == 7
    assert sum(1 for edge in edges if edge.role == module.Q4_EDGE_OUTGOING) == 7
    assert all(edge.count == 1 for edge in edges)

    incoming = [edge for edge in edges if edge.role == module.Q4_EDGE_INCOMING]
    assert {edge.endpoint for edge in incoming} == {
        module.Q4AccountId(bank_id="1", account="SRC")
    }
    assert module.Q4AccountId(bank_id="7", account="M5") in {
        edge.intermediate for edge in incoming
    }

    worker._handle_upstream_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=7),
    )
    assert _eof_counts(worker, output) == {
        f"q4_sum_{partition}": by_partition.get(
            f"q4_sum_{partition}", 0
        )
        for partition in range(4)
    }


def test_source_prefilter_single_eof_discards_unqualified_pending_rows(
    monkeypatch, tmp_path
):
    module, _ = _load_module(monkeypatch, tmp_path, amount=1, edge_partitions=3)
    worker = module.Q4FilterWorker()
    output = worker._edge_store_output
    client_id = 21

    rows = [
        _tx(from_account="PENDING", to_account=f"M{i}", to_bank=f"00{i + 2}")
        for i in range(5)
    ]
    worker._handle_data_packet(client_id, _payload(module, rows))

    state = next(iter(worker._states_by_client[client_id].values()))
    assert len(state.pending) == 5

    worker._handle_upstream_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=5),
    )

    assert _counted_data_messages(worker, output)[0] == []
    assert _eof_counts(worker, output) == {
        "q4_sum_0": 0,
        "q4_sum_1": 0,
        "q4_sum_2": 0,
    }
    assert client_id not in worker._states_by_client
    assert client_id in worker._closed_by_client


def test_source_prefilter_multi_worker_waits_for_flush_order_and_reports_late_data(
    monkeypatch, tmp_path
):
    module, _ = _load_module(monkeypatch, tmp_path, amount=2, worker_id=0)
    worker = module.Q4FilterWorker()
    reports = []
    monkeypatch.setattr(
        worker,
        "_report_to_leader",
        lambda client_id, leader_id, processed_count, forwarded_count: reports.append(
            (client_id, leader_id, processed_count, forwarded_count)
        ),
    )
    client_id = 31

    rows = [_tx(from_account="A", to_account=f"M{i}") for i in range(3)]
    worker._handle_data_packet(client_id, _payload(module, rows))
    worker._handle_upstream_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=4),
    )

    assert reports == [(client_id, 0, 3, 0)]
    assert client_id not in worker._closed_by_client

    worker._handle_data_packet(
        client_id,
        _payload(module, [_tx(from_account="A", to_account="M3")]),
    )
    assert reports[-1] == (client_id, 0, 1, 0)
    assert client_id not in worker._closed_by_client

    acked = []
    nacked = []
    report = worker._packet(
        MessageType.PROCESSED_ANSWER,
        client_id,
        _control_payload(worker, sender_id=1, expected_total=0, processed_count=4),
    )
    worker._handle_leader_report(
        report,
        lambda: acked.append(True),
        lambda: nacked.append(True),
    )

    assert acked == [True]
    assert nacked == []
    assert len(worker._control_sender.sent) == 1
    _, flush_order = worker._control_sender.sent[0]

    worker._handle_flush_order(
        flush_order,
        lambda: acked.append(True),
        lambda: nacked.append(True),
    )

    assert client_id in worker._closed_by_client
    assert _eof_counts(worker, worker._edge_store_output) == {
        "q4_sum_0": 0,
        "q4_sum_1": 0,
        "q4_sum_2": 0,
    }
