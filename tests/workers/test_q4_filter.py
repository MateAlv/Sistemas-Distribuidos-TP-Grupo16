import importlib
import sys

from common.domain.transaction import Transaction
from common.message_protocol.internal import InternalProtocol, Q4CountedEdgeSerializer
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)


class FakeInput:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.closed = False
        self.stop_requested = False

    def send(self, message, routing_key=None):
        raise AssertionError("input endpoint should not publish in these tests")

    def start_consuming(self, callback):
        self.callback = callback

    def request_stop_consuming(self):
        self.stop_requested = True

    def close(self):
        self.closed = True


class RecordingPublisher:
    instances = []

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.sent = []
        RecordingPublisher.instances.append(self)

    def send(self, body):
        self.sent.append((None, body))

    def send_to_shard(self, body, shard):
        self.sent.append((shard, body))

    def close(self):
        pass


class RecordingQueue:
    by_name = {}

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.sent = []
        RecordingQueue.by_name[queue_name] = self

    def send(self, message, routing_key=None):
        if routing_key is not None:
            raise AssertionError("queue sender received a routing key")
        self.sent.append(message)

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


def _load_module(monkeypatch, tmp_path, amount=1, worker_id=0, sum_partitions=3):
    monkeypatch.setenv("ID", str(worker_id))
    monkeypatch.setenv("MOM_HOST", "mom")
    monkeypatch.setenv("INPUT_QUEUE", "q4_filter_input")
    monkeypatch.delenv("Q4_FILTER_INPUT_EXCHANGE", raising=False)
    monkeypatch.setenv("Q4_FILTER_AMOUNT", str(amount))
    monkeypatch.setenv("Q4_FILTER_PREFIX", "q4_filter")
    monkeypatch.setenv("Q4_SUM_EXCHANGE", "q4_sum")
    monkeypatch.setenv("Q4_SUM_AMOUNT", str(sum_partitions))
    monkeypatch.setenv("Q4_SUM_ROUTING_PREFIX", "q4_sum")
    monkeypatch.setenv("Q4_FILTER_BATCH_MAX_EDGES", "5000")
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    monkeypatch.setenv("SNAPSHOT_INTERVAL", "1000")

    RecordingPublisher.instances = []
    RecordingQueue.by_name = {}

    module_name = "workers.scatter_gather.q4_filter.filters"
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)

    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", FakeInput)
    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", FakeInput)
    monkeypatch.setattr(module, "ShardedPublisher", RecordingPublisher)
    monkeypatch.setattr(module, "LazyQueue", RecordingQueue)
    return module


def _worker(module):
    return module.Q4FilterWorker()


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


def _addressed_packet(module, msg_type, client_id, payload, sender_id=9, seq=0):
    return InternalProtocol.create_addressed_packet(
        msg_type,
        client_id.to_bytes(16, "big"),
        sender_id,
        seq,
        payload,
    )


def _data_packet(module, client_id, transactions, sender_id=9, seq=0):
    return _addressed_packet(
        module,
        MessageType.DATA,
        client_id,
        _payload(module, transactions),
        sender_id,
        seq,
    )


def _eof_packet(worker, module, client_id, upstream_id=9, expected_total=0, seq=0):
    return _addressed_packet(
        module,
        MessageType.EOF,
        client_id,
        worker._control_payload(upstream_id, expected_total, 0),
        upstream_id,
        seq,
    )


def _control_packet(worker, msg_type, client_id, sender_id, expected_total=0, processed_count=0):
    return worker._proto.create_packet(
        msg_type=msg_type,
        client_id_bytes=client_id.to_bytes(16, "big"),
        payload=worker._control_payload(sender_id, expected_total, processed_count),
    )


def _q4_sent(worker, module):
    sender = getattr(worker._tls, "senders", {}).get(module.Q4_SUM_EDGE)
    return [] if sender is None else list(sender.sent)


def _counted_data_messages(worker, module):
    edges = []
    by_partition = {}
    seqs_by_partition = {}
    for shard, packet in _q4_sent(worker, module):
        msg_type, client_id, sender_id, seq, payload = worker._proto.unpack_addressed_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        batch = Q4CountedEdgeSerializer.deserialize_batch(payload)
        edges.extend(batch)
        by_partition[shard] = by_partition.get(shard, 0) + len(batch)
        seqs_by_partition.setdefault(shard, []).append(seq)
        assert sender_id == module.ID
        assert client_id >= 0
    return edges, by_partition, seqs_by_partition


def _eof_counts(worker, module):
    counts = {}
    seqs = {}
    for shard, packet in _q4_sent(worker, module):
        msg_type, _client_id, sender_id, seq, payload = worker._proto.unpack_addressed_packet(packet)
        if msg_type != MessageType.EOF:
            continue
        control = worker._control_serializer.deserialize(payload)
        counts[shard] = control.expected_total
        seqs[shard] = seq
        assert sender_id == module.ID
        assert control.sender_id == module.ID
    return counts, seqs


def _queue_messages(name):
    queue = RecordingQueue.by_name.get(name)
    return [] if queue is None else list(queue.sent)


def test_q4_filter_predeclares_sum_bindings(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_partitions=3)
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
            "q4_sum",
            {
                "q4_sum_0": "q4_sum_0",
                "q4_sum_1": "q4_sum_1",
                "q4_sum_2": "q4_sum_2",
            },
        )
    ]


def test_data_uses_source_gate_and_emits_addressed_counted_edges(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, amount=1, sum_partitions=4)
    worker = _worker(module)
    calls = AckNack()
    client_id = 17

    first = [
        _tx(to_bank="002", to_account="M0"),
        _tx(to_bank="002", to_account="M0"),
        _tx(to_bank="003", to_account="M1"),
        _tx(to_bank="004", to_account="M2"),
        _tx(to_bank="005", to_account="M3"),
        _tx(to_bank="006", to_account="M4"),
    ]
    worker._process_data_message(
        _data_packet(module, client_id, first, sender_id=5, seq=0),
        calls.ack,
        calls.nack,
    )
    assert _q4_sent(worker, module) == []

    worker._process_data_message(
        _data_packet(module, client_id, [_tx(to_bank="007", to_account="M5")], sender_id=5, seq=1),
        calls.ack,
        calls.nack,
    )

    edges, by_partition, seqs = _counted_data_messages(worker, module)
    assert calls.acks == 2
    assert calls.nacks == 0
    assert len(edges) == 14
    assert sum(1 for edge in edges if edge.role == module.Q4_EDGE_INCOMING) == 7
    assert sum(1 for edge in edges if edge.role == module.Q4_EDGE_OUTGOING) == 7
    assert all(edge.count == 1 for edge in edges)
    assert {edge.endpoint for edge in edges if edge.role == module.Q4_EDGE_INCOMING} == {
        module.Q4AccountId(bank_id="1", account="SRC")
    }
    assert module.Q4AccountId(bank_id="7", account="M5") in {
        edge.intermediate for edge in edges
    }
    assert sum(by_partition.values()) == 14
    assert all(seqs_for_partition == list(range(len(seqs_for_partition))) for seqs_for_partition in seqs.values())


def test_redelivered_data_is_deduped_without_reapplying_gate(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, amount=1, sum_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    packet = _data_packet(
        module,
        23,
        [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(6)],
        sender_id=2,
        seq=0,
    )

    worker._process_data_message(packet, calls.ack, calls.nack)
    worker._process_data_message(packet, calls.ack, calls.nack)

    edges, _, _ = _counted_data_messages(worker, module)
    assert len(edges) == 12
    assert worker._state.processed_count(23) == 6
    assert calls.acks == 2
    assert calls.nacks == 0


def test_single_eof_discards_unqualified_pending_rows_and_closes(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, amount=1, sum_partitions=3)
    worker = _worker(module)
    calls = AckNack()
    client_id = 21

    worker._process_data_message(
        _data_packet(
            module,
            client_id,
            [_tx(from_account="PENDING", to_account=f"M{i}", to_bank=f"00{i + 2}") for i in range(5)],
            sender_id=3,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    assert len(next(iter(worker._state.source_states_for(client_id).values())).pending) == 5

    worker._process_data_message(
        _eof_packet(worker, module, client_id, upstream_id=3, expected_total=5, seq=1),
        calls.ack,
        calls.nack,
    )

    assert _counted_data_messages(worker, module)[0] == []
    assert _eof_counts(worker, module)[0] == {0: 0, 1: 0, 2: 0}
    assert worker._state.source_states_for(client_id) == {}
    assert worker._state.is_closed(client_id)
    assert calls.nacks == 0


def test_eof_counts_match_forwarded_edges_after_qualification(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, amount=1, sum_partitions=4)
    worker = _worker(module)
    calls = AckNack()
    client_id = 24

    worker._process_data_message(
        _data_packet(
            module,
            client_id,
            [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(6)],
            sender_id=1,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    _edges, by_partition, data_seqs = _counted_data_messages(worker, module)

    worker._process_data_message(
        _eof_packet(worker, module, client_id, upstream_id=1, expected_total=6, seq=1),
        calls.ack,
        calls.nack,
    )

    eof_counts, eof_seqs = _eof_counts(worker, module)
    assert eof_counts == {
        partition: by_partition.get(partition, 0) for partition in range(4)
    }
    for partition, eof_seq in eof_seqs.items():
        assert eof_seq == len(data_seqs.get(partition, []))
    assert worker._state.is_closed(client_id)


def test_recovery_restores_pending_gate_state(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path)
    worker = _worker(module)
    calls = AckNack()
    client_id = 30
    worker._process_data_message(
        _data_packet(
            module,
            client_id,
            [_tx(from_account="A", to_account=f"M{i}", to_bank=f"00{i + 2}") for i in range(5)],
            sender_id=7,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )

    recovered = _worker(module)

    states = recovered._state.source_states_for(client_id)
    assert len(states) == 1
    assert len(next(iter(states.values())).pending) == 5
    assert recovered._state.processed_count(client_id) == 5


def test_recovery_republishes_data_outbox_after_crash_before_publish(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_partitions=3)
    worker = _worker(module)
    calls = AckNack()
    packet = _data_packet(
        module,
        31,
        [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(6)],
        sender_id=4,
        seq=0,
    )

    def crash_publish(_entries):
        raise RuntimeError("crash after INPUT_APPLIED")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._process_data_message(packet, calls.ack, calls.nack)
    assert calls.acks == 0
    assert calls.nacks == 1

    recovered = _worker(module)
    edges, _, _ = _counted_data_messages(recovered, module)
    assert len(edges) == 12
    assert recovered._state.processed_count(31) == 6


def test_recovery_republishes_data_outbox_after_crash_before_done(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_partitions=3)
    worker = _worker(module)
    calls = AckNack()
    packet = _data_packet(
        module,
        32,
        [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(6)],
        sender_id=4,
        seq=0,
    )
    original_commit = worker._handler.commit_done

    def crash_commit(*args, **kwargs):
        raise RuntimeError("crash after publish before INPUT_DONE")

    monkeypatch.setattr(worker._handler, "commit_done", crash_commit)
    worker._process_data_message(packet, calls.ack, calls.nack)
    assert calls.acks == 0
    assert calls.nacks == 1
    assert len(_counted_data_messages(worker, module)[0]) == 12
    monkeypatch.setattr(worker._handler, "commit_done", original_commit)

    recovered = _worker(module)
    edges, _, _ = _counted_data_messages(recovered, module)
    assert len(edges) == 12
    assert recovered._state.processed_count(32) == 6


def test_recovery_after_done_before_ack_does_not_reapply_or_republish(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, sum_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    packet = _data_packet(
        module,
        33,
        [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(6)],
        sender_id=4,
        seq=0,
    )

    def crash_ack():
        raise RuntimeError("crash after INPUT_DONE before Rabbit ack")

    worker._process_data_message(packet, crash_ack, calls.nack)
    assert calls.nacks == 1

    recovered = _worker(module)
    assert _q4_sent(recovered, module) == []
    redelivery = AckNack()
    recovered._process_data_message(packet, redelivery.ack, redelivery.nack)
    assert redelivery.acks == 1
    assert redelivery.nacks == 0
    assert recovered._state.processed_count(33) == 6
    assert _q4_sent(recovered, module) == []


def test_recovery_republishes_eof_outbox_and_preserves_close(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, amount=1, sum_partitions=3)
    worker = _worker(module)
    calls = AckNack()
    client_id = 34
    worker._process_data_message(
        _data_packet(
            module,
            client_id,
            [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(6)],
            sender_id=8,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )

    def crash_publish(_entries):
        raise RuntimeError("crash after EOF INPUT_APPLIED")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._process_data_message(
        _eof_packet(worker, module, client_id, upstream_id=8, expected_total=6, seq=1),
        calls.ack,
        calls.nack,
    )

    recovered = _worker(module)
    assert recovered._state.is_closed(client_id)
    eof_counts, _ = _eof_counts(recovered, module)
    assert set(eof_counts) == {0, 1, 2}
    assert sum(eof_counts.values()) == 12


def test_non_leader_reports_then_flush_order_closes_durably(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, amount=2, worker_id=1, sum_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 40
    worker._process_data_message(
        _data_packet(
            module,
            client_id,
            [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(3)],
            sender_id=5,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    worker._process_data_message(
        _eof_packet(worker, module, client_id, upstream_id=5, expected_total=4, seq=1),
        calls.ack,
        calls.nack,
    )

    reports = _queue_messages("q4_filter_response_0")
    assert len(reports) == 1
    msg_type, reported_client, payload = worker._proto.unpack_packet(reports[0])
    report = worker._control_serializer.deserialize(payload)
    assert msg_type == MessageType.PROCESSED_ANSWER
    assert reported_client == client_id
    assert report.sender_id == 1
    assert report.processed_count == 3
    assert not worker._state.is_closed(client_id)

    control_calls = AckNack()
    worker._handle_control(
        _control_packet(worker, MessageType.FLUSH_ORDER, client_id, sender_id=0),
        control_calls.ack,
        control_calls.nack,
    )

    assert control_calls.acks == 1
    assert worker._state.is_closed(client_id)
    assert set(_eof_counts(worker, module)[0]) == {0, 1}
    flush_acks = _queue_messages("q4_filter_response_0")
    assert len(flush_acks) == 2
    msg_type, ack_client, payload = worker._proto.unpack_packet(flush_acks[1])
    flush_ack = worker._control_serializer.deserialize(payload)
    assert msg_type == MessageType.FLUSH_ACK
    assert ack_client == client_id
    assert flush_ack.sender_id == 1


def test_leader_broadcasts_flush_order_and_flush_ack_closes(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, amount=2, worker_id=0, sum_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 41
    worker._process_data_message(
        _data_packet(
            module,
            client_id,
            [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(6)],
            sender_id=5,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    worker._process_data_message(
        _eof_packet(worker, module, client_id, upstream_id=5, expected_total=6, seq=1),
        calls.ack,
        calls.nack,
    )

    self_report = _queue_messages("q4_filter_response_0")[0]
    worker._handle_response(self_report, calls.ack, calls.nack)
    worker._handle_response(
        _control_packet(
            worker,
            MessageType.PROCESSED_ANSWER,
            client_id,
            sender_id=1,
            expected_total=0,
            processed_count=0,
        ),
        calls.ack,
        calls.nack,
    )

    assert len(_queue_messages("q4_filter_control_0")) == 1
    assert len(_queue_messages("q4_filter_control_1")) == 1
    assert not worker._state.is_closed(client_id)

    worker._handle_response(
        _control_packet(worker, MessageType.FLUSH_ACK, client_id, sender_id=1),
        calls.ack,
        calls.nack,
    )

    assert worker._state.is_closed(client_id)
    eof_counts, _ = _eof_counts(worker, module)
    assert set(eof_counts) == {0, 1}
    assert sum(eof_counts.values()) == 12


def test_recovery_republishes_flush_order_outputs_before_commit(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, amount=2, worker_id=1, sum_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 42
    worker._process_data_message(
        _data_packet(
            module,
            client_id,
            [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(6)],
            sender_id=5,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    worker._process_data_message(
        _eof_packet(worker, module, client_id, upstream_id=5, expected_total=6, seq=1),
        calls.ack,
        calls.nack,
    )

    def crash_publish(_entries):
        raise RuntimeError("crash after FLUSH_ORDER INPUT_APPLIED")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._handle_control(
        _control_packet(worker, MessageType.FLUSH_ORDER, client_id, sender_id=0),
        calls.ack,
        calls.nack,
    )

    recovered = _worker(module)
    assert recovered._state.is_closed(client_id)
    eof_counts, _ = _eof_counts(recovered, module)
    assert set(eof_counts) == {0, 1}
    assert len(_queue_messages("q4_filter_response_0")) >= 1


def test_recovery_republishes_leader_flush_ack_outputs_before_commit(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, amount=2, worker_id=0, sum_partitions=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 43
    worker._process_data_message(
        _data_packet(
            module,
            client_id,
            [_tx(to_bank=f"00{i + 2}", to_account=f"M{i}") for i in range(6)],
            sender_id=5,
            seq=0,
        ),
        calls.ack,
        calls.nack,
    )
    worker._process_data_message(
        _eof_packet(worker, module, client_id, upstream_id=5, expected_total=6, seq=1),
        calls.ack,
        calls.nack,
    )

    def crash_publish(_entries):
        raise RuntimeError("crash after FLUSH_ACK INPUT_APPLIED")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._handle_response(
        _control_packet(worker, MessageType.FLUSH_ACK, client_id, sender_id=1),
        calls.ack,
        calls.nack,
    )

    recovered = _worker(module)
    assert recovered._state.is_closed(client_id)
    eof_counts, _ = _eof_counts(recovered, module)
    assert set(eof_counts) == {0, 1}
    assert sum(eof_counts.values()) == 12
