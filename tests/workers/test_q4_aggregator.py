import importlib
import sys

from common.message_protocol.internal import Q4PairPaths
from common.message_protocol.internal.common import MessageType


def _load_module(
    monkeypatch,
    *,
    worker_id=0,
    joiner_amount=1,
    account_partitions=3,
):
    monkeypatch.setenv("ID", str(worker_id))
    monkeypatch.setenv("MOM_HOST", "mom")
    monkeypatch.setenv("Q4_AGGREGATOR_EXCHANGE", "q4_aggregator")
    monkeypatch.setenv("Q4_AGGREGATOR_ROUTING_PREFIX", "q4_aggregator")
    monkeypatch.setenv("Q4_JOINER_AMOUNT", str(joiner_amount))
    monkeypatch.setenv("Q4_DEDUPER_EXCHANGE", "q4_deduper")
    monkeypatch.setenv("Q4_DEDUPER_AMOUNT", str(account_partitions))
    monkeypatch.setenv("Q4_DEDUPER_ROUTING_PREFIX", "q4_deduper")
    monkeypatch.setenv("Q4_AGGREGATOR_BATCH_BYTES", str(1024 * 1024))
    monkeypatch.setenv("Q4_AGGREGATOR_BATCH_MAX_ACCOUNTS", "5000")

    module_name = "workers.scatter_gather.q4_aggregator.aggregator"
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


def _delta(module, source, target, path_count):
    return Q4PairPaths(source=source, target=target, path_count=path_count)


def _data_payload(module, deltas):
    return module.Q4PairPathsSerializer.serialize_batch(deltas)


def _control_payload(worker, sender_id, expected_total, processed_count=0):
    return worker._control_payload(sender_id, expected_total, processed_count)


def _account_data_messages(worker, module, output):
    accounts = []
    by_partition = {}
    for routing_key, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        batch = module.Q4AccountIdSerializer.deserialize_batch(payload)
        accounts.extend(batch)
        by_partition[routing_key] = by_partition.get(routing_key, 0) + len(batch)
    return accounts, by_partition


def _eof_counts(worker, output):
    counts = {}
    for routing_key, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.EOF:
            continue
        control = worker._control_serializer.deserialize(payload)
        counts[routing_key] = control.expected_total
    return counts


def test_q4_aggregator_predeclares_deduper_bindings(monkeypatch):
    module, _ = _load_module(monkeypatch, account_partitions=3)
    calls = []
    monkeypatch.setattr(
        module,
        "ensure_exchange_queue_bindings",
        lambda *args: calls.append(args),
    )

    module.Q4AggregatorWorker()._ensure_output_bindings()

    assert calls == [
        (
            "mom",
            "q4_deduper",
            {
                "q4_deduper_0": "q4_deduper_0",
                "q4_deduper_1": "q4_deduper_1",
                "q4_deduper_2": "q4_deduper_2",
            },
        )
    ]


def test_pair_reducer_emits_candidates_once_when_pair_reaches_threshold(
    monkeypatch,
):
    module, _ = _load_module(
        monkeypatch,
        joiner_amount=2,
        account_partitions=4,
    )
    worker = module.Q4AggregatorWorker()
    output = worker._account_deduper_output
    client_id = 61

    a = _account(module, "1", "A")
    b = _account(module, "2", "B")

    worker._accept_pair_paths(
        client_id,
        _data_payload(
            module,
            [
                _delta(module, a, b, 2),
                _delta(module, a, b, 3),
            ],
        ),
    )
    worker._flush_client_buffers(client_id)
    assert output.sent == []

    worker._accept_pair_paths(
        client_id,
        _data_payload(
            module,
            [
                _delta(module, a, b, 1),
                _delta(module, a, b, 6),
            ],
        ),
    )
    worker._flush_client_buffers(client_id)

    accounts, by_partition = _account_data_messages(worker, module, output)
    assert accounts == [a, b]

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=4),
    )
    assert client_id not in worker._closed_by_client

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=1, expected_total=0),
    )

    assert _eof_counts(worker, output) == {
        f"q4_deduper_{partition}": by_partition.get(
            f"q4_deduper_{partition}", 0
        )
        for partition in range(4)
    }
    assert client_id in worker._closed_by_client


def test_pair_reducer_keeps_subthreshold_counts_in_memory_until_eof(monkeypatch):
    module, _ = _load_module(
        monkeypatch,
        joiner_amount=1,
        account_partitions=3,
    )
    worker = module.Q4AggregatorWorker()
    output = worker._account_deduper_output
    client_id = 62

    a = _account(module, "1", "A")
    b = _account(module, "2", "B")
    c = _account(module, "3", "C")
    d = _account(module, "4", "D")

    worker._accept_pair_paths(
        client_id,
        _data_payload(
            module,
            [
                _delta(module, a, b, 5),
                _delta(module, c, d, 5),
            ],
        ),
    )

    assert worker._pair_counts_by_client[client_id] == {
        ("1", "A", "2", "B"): 5,
        ("3", "C", "4", "D"): 5,
    }
    assert _account_data_messages(worker, module, output)[0] == []

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=4),
    )

    accounts, by_partition = _account_data_messages(worker, module, output)
    assert accounts == []
    assert sum(by_partition.values()) == 0
    assert sum(_eof_counts(worker, output).values()) == 0
    assert client_id in worker._closed_by_client


def test_pair_reducer_ignores_self_pairs_duplicate_eof_and_late_data(monkeypatch):
    module, _ = _load_module(
        monkeypatch,
        joiner_amount=1,
        account_partitions=2,
    )
    worker = module.Q4AggregatorWorker()
    output = worker._account_deduper_output
    client_id = 63

    a = _account(module, "1", "A")
    b = _account(module, "2", "B")
    late = _account(module, "9", "LATE")

    worker._accept_pair_paths(
        client_id,
        _data_payload(
            module,
            [
                _delta(module, a, a, 6),
                _delta(module, a, b, 6),
            ],
        ),
    )
    eof_payload = _control_payload(worker, sender_id=0, expected_total=2)
    worker._handle_eof(client_id, eof_payload)
    emitted_after_close = len(output.sent)

    accounts, _ = _account_data_messages(worker, module, output)
    assert accounts == [a, b]

    worker._handle_eof(client_id, eof_payload)
    worker._accept_pair_paths(
        client_id,
        _data_payload(module, [_delta(module, a, late, 6)]),
    )

    assert len(output.sent) == emitted_after_close
