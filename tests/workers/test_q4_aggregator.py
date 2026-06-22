import importlib
import sys

from common.message_protocol.internal import InternalProtocol, Q4PairPaths
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
    monkeypatch.setenv("Q4_AGGREGATOR_BATCH_MAX_ACCOUNTS", "5000")
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    monkeypatch.setenv("SNAPSHOT_INTERVAL", "1000")

    state_module = "workers.scatter_gather.q4_aggregator.q4_aggregator_state"
    module_name = "workers.scatter_gather.q4_aggregator.aggregator"
    sys.modules.pop(module_name, None)
    sys.modules.pop(state_module, None)
    module = importlib.import_module(module_name)

    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    monkeypatch.setattr(module, "ShardedPublisher", RecordingPublisher)
    return module


def _worker(module):
    return module.Q4AggregatorWorker()


def _account(module, bank, account):
    return module.Q4AccountId(bank_id=bank, account=account)


def _delta(module, source, target, path_count):
    return Q4PairPaths(source=source, target=target, path_count=path_count)


def _data_packet(module, client_id, deltas, sender_id=0, seq=0):
    return InternalProtocol.create_addressed_packet(
        MessageType.DATA,
        client_id.to_bytes(16, "big"),
        sender_id,
        seq,
        module.Q4PairPathsSerializer.serialize_batch(deltas),
    )


def _eof_packet(worker, client_id, upstream_id, expected_total=0, seq=0):
    return InternalProtocol.create_addressed_packet(
        MessageType.EOF,
        client_id.to_bytes(16, "big"),
        upstream_id,
        seq,
        worker._control_payload(upstream_id, expected_total, 0),
    )


def _account_data_messages(worker, module):
    accounts = []
    by_partition = {}
    for shard, packet in worker._account_deduper_output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        batch = module.Q4AccountIdSerializer.deserialize_batch(payload)
        accounts.extend(batch)
        by_partition[shard] = by_partition.get(shard, 0) + len(batch)
    return accounts, by_partition


def _eof_counts(worker, module):
    counts = {}
    for shard, packet in worker._account_deduper_output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.EOF:
            continue
        control = worker._control_serializer.deserialize(payload)
        assert control.sender_id == module.ID
        counts[shard] = control.expected_total
    return counts


def test_q4_aggregator_predeclares_deduper_bindings(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, account_partitions=3)
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
            "q4_deduper",
            {
                "q4_deduper_0": "q4_deduper_0",
                "q4_deduper_1": "q4_deduper_1",
                "q4_deduper_2": "q4_deduper_2",
            },
        )
    ]


def test_data_emits_candidates_once_when_pair_reaches_threshold(monkeypatch, tmp_path):
    module = _load_module(
        monkeypatch,
        tmp_path,
        joiner_amount=2,
        account_partitions=4,
    )
    worker = _worker(module)
    calls = AckNack()
    client_id = 61
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_delta(module, a, b, 2), _delta(module, a, b, 3)],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    assert worker._account_deduper_output.sent == []

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_delta(module, a, b, 1), _delta(module, a, b, 6)],
            sender_id=0,
            seq=1,
        ),
        calls.ack,
        calls.nack,
    )

    accounts, by_partition = _account_data_messages(worker, module)
    assert accounts == [a, b]
    assert worker._state.forwarded_total(client_id) == 2

    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=4, seq=2),
        calls.ack,
        calls.nack,
    )
    assert not worker._state.is_closed(client_id)

    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=1, expected_total=0, seq=0),
        calls.ack,
        calls.nack,
    )

    assert _eof_counts(worker, module) == {
        partition: by_partition.get(partition, 0) for partition in range(4)
    }
    assert worker._state.is_closed(client_id)
    assert calls.nacks == 0


def test_subthreshold_pairs_are_dropped_on_eof(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, joiner_amount=1, account_partitions=3)
    worker = _worker(module)
    calls = AckNack()
    client_id = 62
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")
    c = _account(module, "3", "C")
    d = _account(module, "4", "D")

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_delta(module, a, b, 5), _delta(module, c, d, 5)],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )

    assert worker._state.pair_counts_for(client_id) == {
        ("1", "A", "2", "B"): 5,
        ("3", "C", "4", "D"): 5,
    }
    assert _account_data_messages(worker, module)[0] == []

    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=2, seq=1),
        calls.ack,
        calls.nack,
    )

    accounts, by_partition = _account_data_messages(worker, module)
    assert accounts == []
    assert sum(by_partition.values()) == 0
    assert _eof_counts(worker, module) == {0: 0, 1: 0, 2: 0}
    assert worker._state.is_closed(client_id)
    assert calls.nacks == 0


def test_ignores_self_pairs_duplicate_eof_and_late_data(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, joiner_amount=1, account_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 63
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")
    late = _account(module, "9", "LATE")

    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_delta(module, a, a, 6), _delta(module, a, b, 6)],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    eof = _eof_packet(worker, client_id, upstream_id=0, expected_total=2, seq=1)
    worker._on_message(eof, calls.ack, calls.nack)
    emitted_after_close = len(worker._account_deduper_output.sent)

    accounts, _ = _account_data_messages(worker, module)
    assert accounts == [a, b]

    worker._on_message(eof, calls.ack, calls.nack)
    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_delta(module, a, late, 6)],
            sender_id=0,
            seq=2,
        ),
        calls.ack,
        calls.nack,
    )

    assert len(worker._account_deduper_output.sent) == emitted_after_close
    assert calls.nacks == 0


def test_recovery_restores_accumulated_pair_counts(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, joiner_amount=2)
    worker = _worker(module)
    calls = AckNack()
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")

    worker._on_message(
        _data_packet(
            module,
            64,
            [_delta(module, a, b, 4)],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )

    recovered = _worker(module)
    assert recovered._state.processed_count(64) == 1
    assert recovered._state.pair_counts_for(64)[("1", "A", "2", "B")] == 4
    assert recovered._state.forwarded_total(64) == 0


def test_recovery_republishes_data_outbox_after_crash_before_publish(
    monkeypatch,
    tmp_path,
):
    module = _load_module(monkeypatch, tmp_path, joiner_amount=1, account_partitions=3)
    worker = _worker(module)
    calls = AckNack()
    client_id = 65
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")

    def crash_publish(_entries):
        raise RuntimeError("crash after DATA INPUT_APPLIED")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_delta(module, a, b, 6)],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1

    recovered = _worker(module)
    assert _account_data_messages(recovered, module)[0] == [a, b]
    assert recovered._state.forwarded_total(client_id) == 2


def test_recovery_republishes_data_outbox_after_publish_before_done(
    monkeypatch,
    tmp_path,
):
    module = _load_module(monkeypatch, tmp_path, joiner_amount=1, account_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 66
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")

    def crash_commit(*_args, **_kwargs):
        raise RuntimeError("crash after publish before INPUT_DONE")

    monkeypatch.setattr(worker._handler, "commit_done", crash_commit)
    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_delta(module, a, b, 6)],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1
    assert _account_data_messages(worker, module)[0] == [a, b]

    recovered = _worker(module)
    assert _account_data_messages(recovered, module)[0] == [a, b]
    assert recovered._state.forwarded_total(client_id) == 2


def test_recovery_after_data_done_before_ack_does_not_reapply_or_republish(
    monkeypatch,
    tmp_path,
):
    module = _load_module(monkeypatch, tmp_path, joiner_amount=1)
    worker = _worker(module)
    calls = AckNack()
    client_id = 67
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")
    packet = _data_packet(
        module,
        client_id,
        [_delta(module, a, b, 4)],
        sender_id=0,
        seq=0,
    )

    def crash_ack():
        raise RuntimeError("crash after INPUT_DONE before ack")

    worker._on_message(packet, crash_ack, calls.nack)
    assert calls.nacks == 1

    recovered = _worker(module)
    assert recovered._account_deduper_output.sent == []
    redelivery = AckNack()
    recovered._on_message(packet, redelivery.ack, redelivery.nack)
    assert redelivery.acks == 1
    assert redelivery.nacks == 0
    assert recovered._state.processed_count(client_id) == 1
    assert recovered._account_deduper_output.sent == []


def test_recovery_republishes_eof_outbox_after_crash_before_publish(
    monkeypatch,
    tmp_path,
):
    module = _load_module(monkeypatch, tmp_path, joiner_amount=1, account_partitions=3)
    worker = _worker(module)
    calls = AckNack()
    client_id = 68
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")
    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_delta(module, a, b, 6)],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    _, by_partition = _account_data_messages(worker, module)

    def crash_publish(_entries):
        raise RuntimeError("crash after EOF INPUT_APPLIED")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=1, seq=1),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1

    recovered = _worker(module)
    assert _eof_counts(recovered, module) == {
        partition: by_partition.get(partition, 0) for partition in range(3)
    }
    assert recovered._state.is_closed(client_id)


def test_recovery_republishes_eof_outbox_after_publish_before_done(
    monkeypatch,
    tmp_path,
):
    module = _load_module(monkeypatch, tmp_path, joiner_amount=1, account_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 69
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")
    worker._on_message(
        _data_packet(
            module,
            client_id,
            [_delta(module, a, b, 6)],
            sender_id=0,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )

    def crash_commit(*_args, **_kwargs):
        raise RuntimeError("crash after EOF publish before INPUT_DONE")

    monkeypatch.setattr(worker._handler, "commit_done", crash_commit)
    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=1, seq=1),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1
    assert set(_eof_counts(worker, module)) == {0, 1}

    recovered = _worker(module)
    assert set(_eof_counts(recovered, module)) == {0, 1}
    assert recovered._state.is_closed(client_id)
