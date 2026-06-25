import importlib
import sys

from common.message_protocol.internal import InternalProtocol
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


class RecordingQueue:
    by_name = {}

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.sent = []
        self.closed = False
        self.stop_requested = False
        RecordingQueue.by_name.setdefault(queue_name, []).append(self)

    def send(self, message, routing_key=None):
        assert routing_key is None
        self.sent.append((routing_key, message))

    def start_consuming(self, callback):
        self.callback = callback

    def request_stop_consuming(self):
        self.stop_requested = True

    def close(self):
        self.closed = True


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
    aggregator_amount=1,
    deduper_amount=1,
    batch_max=5000,
):
    monkeypatch.setenv("ID", str(worker_id))
    monkeypatch.setenv("MOM_HOST", "mom")
    monkeypatch.setenv("Q4_DEDUPER_EXCHANGE", "q4_deduper")
    monkeypatch.setenv("Q4_DEDUPER_ROUTING_PREFIX", "q4_deduper")
    monkeypatch.setenv("Q4_AGGREGATOR_AMOUNT", str(aggregator_amount))
    monkeypatch.setenv("Q4_DEDUPER_AMOUNT", str(deduper_amount))
    monkeypatch.setenv(
        "Q4_DEDUPER_RESPONSE_QUEUE_PREFIX",
        "q4_deduper_response",
    )
    monkeypatch.setenv("GATEWAY_Q4_QUEUE", "gateway_q4_queue")
    monkeypatch.setenv("Q4_DEDUPER_BATCH_MAX_ACCOUNTS", str(batch_max))
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    monkeypatch.setenv("SNAPSHOT_INTERVAL", "1000")

    RecordingQueue.by_name = {}

    state_module = "workers.scatter_gather.q4_deduper.q4_deduper_state"
    module_name = "workers.scatter_gather.q4_deduper.deduper"
    sys.modules.pop(module_name, None)
    sys.modules.pop(state_module, None)
    module = importlib.import_module(module_name)

    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", RecordingQueue)
    return module


def _worker(module):
    return module.Q4DeduperWorker()


def _account(module, bank, account):
    return module.Q4AccountId(bank_id=bank, account=account)


def _data_packet(module, client_id, accounts, sender_id=0, seq=0):
    return InternalProtocol.create_addressed_packet(
        MessageType.DATA,
        client_id.to_bytes(16, "big"),
        sender_id,
        seq,
        module.Q4AccountIdSerializer.serialize_batch(accounts),
    )


def _eof_packet(worker, client_id, upstream_id, expected_total=0, seq=0):
    return InternalProtocol.create_addressed_packet(
        MessageType.EOF,
        client_id.to_bytes(16, "big"),
        upstream_id,
        seq,
        worker._control_payload(upstream_id, expected_total, 0),
    )


def _leader_report_packet(worker, client_id, sender_id, emitted_accounts):
    return worker._packet(
        MessageType.EOF_RECEIVED,
        client_id,
        worker._control_payload(sender_id, emitted_accounts, 0),
    )


def _gateway_accounts(worker, module, output=None):
    output = output or worker._gateway_output
    accounts = []
    for _, packet in output.sent:
        msg_type, _, _, _, payload = worker._proto.unpack_addressed_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        accounts.extend(module.Q4AccountIdSerializer.deserialize_batch(payload))
    return accounts


def _gateway_eof_totals(worker, output=None):
    output = output or worker._gateway_output
    totals = []
    for _, packet in output.sent:
        msg_type, _, _, _, payload = worker._proto.unpack_addressed_packet(packet)
        if msg_type != MessageType.EOF:
            continue
        control = worker._control_serializer.deserialize(payload)
        totals.append(control.expected_total)
    return totals


def _leader_reports(worker):
    reports = []
    output = worker._leader_output
    if output is None:
        return reports
    for _, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.EOF_RECEIVED:
            continue
        reports.append(worker._control_serializer.deserialize(payload))
    return reports


def test_deduper_deduplicates_and_sends_single_gateway_eof(monkeypatch, tmp_path):
    module = _load_module(
        monkeypatch,
        tmp_path,
        aggregator_amount=1,
        deduper_amount=1,
        batch_max=2,
    )
    worker = _worker(module)
    calls = AckNack()
    client_id = 71
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")

    worker._on_message(
        _data_packet(module, client_id, [b, a, b], sender_id=0, seq=0),
        calls.ack,
        calls.nack,
    )
    assert worker._state.accounts_for(client_id) == {("1", "A"), ("2", "B")}
    assert worker._gateway_output.sent == []

    eof = _eof_packet(worker, client_id, upstream_id=0, expected_total=3, seq=1)
    worker._on_message(eof, calls.ack, calls.nack)

    assert _gateway_accounts(worker, module) == [a, b]
    assert _gateway_eof_totals(worker) == [2]
    assert worker._state.is_closed(client_id)

    sent_after_close = len(worker._gateway_output.sent)
    worker._on_message(eof, calls.ack, calls.nack)
    worker._on_message(
        _data_packet(module, client_id, [_account(module, "9", "L")], sender_id=0, seq=2),
        calls.ack,
        calls.nack,
    )
    assert len(worker._gateway_output.sent) == sent_after_close
    assert calls.nacks == 0


def test_waits_for_all_aggregator_eofs_and_reports_leader(monkeypatch, tmp_path):
    module = _load_module(
        monkeypatch,
        tmp_path,
        worker_id=1,
        aggregator_amount=2,
        deduper_amount=3,
    )
    worker = _worker(module)
    calls = AckNack()
    client_id = 72
    account = _account(module, "7", "OWNER")
    worker._on_message(
        _data_packet(module, client_id, [account, account], sender_id=0, seq=0),
        calls.ack,
        calls.nack,
    )

    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=2, seq=1),
        calls.ack,
        calls.nack,
    )
    assert worker._gateway_output.sent == []
    assert worker._leader_output.sent == []

    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=1, expected_total=0, seq=0),
        calls.ack,
        calls.nack,
    )

    assert _gateway_accounts(worker, module) == [account]
    assert _gateway_eof_totals(worker) == []
    reports = _leader_reports(worker)
    assert [(r.sender_id, r.expected_total) for r in reports] == [(1, 1)]
    assert worker._state.is_closed(client_id)
    assert calls.nacks == 0


def test_leader_sends_one_gateway_eof_after_all_reports(monkeypatch, tmp_path):
    module = _load_module(
        monkeypatch,
        tmp_path,
        worker_id=0,
        aggregator_amount=1,
        deduper_amount=3,
    )
    worker = _worker(module)
    calls = AckNack()
    client_id = 73

    worker._on_leader_message(
        _leader_report_packet(worker, client_id, sender_id=0, emitted_accounts=2),
        calls.ack,
        calls.nack,
    )
    worker._on_leader_message(
        _leader_report_packet(worker, client_id, sender_id=1, emitted_accounts=3),
        calls.ack,
        calls.nack,
    )
    worker._on_leader_message(
        _leader_report_packet(worker, client_id, sender_id=1, emitted_accounts=9),
        calls.ack,
        calls.nack,
    )
    assert _gateway_eof_totals(worker, worker._gateway_eof_output) == []

    worker._on_leader_message(
        _leader_report_packet(worker, client_id, sender_id=2, emitted_accounts=4),
        calls.ack,
        calls.nack,
    )

    assert _gateway_eof_totals(worker, worker._gateway_eof_output) == [9]
    assert worker._state.is_leader_closed(client_id)
    assert calls.nacks == 0


def test_recovery_restores_accumulated_accounts(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, aggregator_amount=2)
    worker = _worker(module)
    calls = AckNack()
    client_id = 74
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")

    worker._on_message(
        _data_packet(module, client_id, [a, b, a], sender_id=0, seq=0),
        calls.ack,
        calls.nack,
    )

    recovered = _worker(module)
    assert recovered._state.processed_count(client_id) == 3
    assert recovered._state.accounts_for(client_id) == {("1", "A"), ("2", "B")}


def test_recovery_republishes_close_outputs_after_crash_before_publish(
    monkeypatch,
    tmp_path,
):
    module = _load_module(monkeypatch, tmp_path, aggregator_amount=1, deduper_amount=1)
    worker = _worker(module)
    calls = AckNack()
    client_id = 75
    a = _account(module, "1", "A")
    b = _account(module, "2", "B")
    worker._on_message(
        _data_packet(module, client_id, [b, a, b], sender_id=0, seq=0),
        calls.ack,
        calls.nack,
    )

    def crash_publish(_entries):
        raise RuntimeError("crash after EOF INPUT_APPLIED")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=3, seq=1),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1

    recovered = _worker(module)
    assert _gateway_accounts(recovered, module) == [a, b]
    assert _gateway_eof_totals(recovered) == [2]
    assert recovered._state.is_closed(client_id)


def test_recovery_republishes_close_outputs_after_publish_before_done(
    monkeypatch,
    tmp_path,
):
    module = _load_module(monkeypatch, tmp_path, aggregator_amount=1, deduper_amount=1)
    worker = _worker(module)
    calls = AckNack()
    client_id = 76
    account = _account(module, "1", "A")
    worker._on_message(
        _data_packet(module, client_id, [account], sender_id=0, seq=0),
        calls.ack,
        calls.nack,
    )

    def crash_commit(*_args, **_kwargs):
        raise RuntimeError("crash after close publish before INPUT_DONE")

    monkeypatch.setattr(worker._handler, "commit_done", crash_commit)
    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=1, seq=1),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1
    assert _gateway_accounts(worker, module) == [account]
    assert _gateway_eof_totals(worker) == [1]

    recovered = _worker(module)
    assert _gateway_accounts(recovered, module) == [account]
    assert _gateway_eof_totals(recovered) == [1]
    assert recovered._state.is_closed(client_id)


def test_recovery_after_data_done_before_ack_does_not_reapply(monkeypatch, tmp_path):
    module = _load_module(monkeypatch, tmp_path, aggregator_amount=1)
    worker = _worker(module)
    calls = AckNack()
    client_id = 77
    account = _account(module, "1", "A")
    packet = _data_packet(module, client_id, [account], sender_id=0, seq=0)

    def crash_ack():
        raise RuntimeError("crash after INPUT_DONE before ack")

    worker._on_message(packet, crash_ack, calls.nack)
    assert calls.nacks == 1

    recovered = _worker(module)
    redelivery = AckNack()
    recovered._on_message(packet, redelivery.ack, redelivery.nack)
    assert redelivery.acks == 1
    assert redelivery.nacks == 0
    assert recovered._state.processed_count(client_id) == 1
    assert recovered._state.accounts_for(client_id) == {("1", "A")}


def test_recovery_republishes_non_leader_report_after_crash_before_publish(
    monkeypatch,
    tmp_path,
):
    module = _load_module(
        monkeypatch,
        tmp_path,
        worker_id=2,
        aggregator_amount=1,
        deduper_amount=3,
    )
    worker = _worker(module)
    calls = AckNack()
    client_id = 78
    account = _account(module, "1", "A")
    worker._on_message(
        _data_packet(module, client_id, [account], sender_id=0, seq=0),
        calls.ack,
        calls.nack,
    )

    def crash_publish(_entries):
        raise RuntimeError("crash before leader report publish")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._on_message(
        _eof_packet(worker, client_id, upstream_id=0, expected_total=1, seq=1),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1

    recovered = _worker(module)
    assert _gateway_accounts(recovered, module) == [account]
    reports = _leader_reports(recovered)
    assert [(r.sender_id, r.expected_total) for r in reports] == [(2, 1)]
    assert recovered._state.is_closed(client_id)


def test_recovery_republishes_leader_gateway_eof_after_crash_before_publish(
    monkeypatch,
    tmp_path,
):
    module = _load_module(
        monkeypatch,
        tmp_path,
        worker_id=0,
        aggregator_amount=1,
        deduper_amount=2,
    )
    worker = _worker(module)
    calls = AckNack()
    client_id = 79
    worker._on_leader_message(
        _leader_report_packet(worker, client_id, sender_id=0, emitted_accounts=2),
        calls.ack,
        calls.nack,
    )

    def crash_publish(_entries):
        raise RuntimeError("crash after leader INPUT_APPLIED")

    monkeypatch.setattr(worker, "_publish", crash_publish)
    worker._on_leader_message(
        _leader_report_packet(worker, client_id, sender_id=1, emitted_accounts=3),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1

    recovered = _worker(module)
    assert _gateway_eof_totals(recovered, recovered._gateway_eof_output) == [5]
    assert recovered._state.is_leader_closed(client_id)


def test_recovery_republishes_leader_gateway_eof_after_publish_before_done(
    monkeypatch,
    tmp_path,
):
    module = _load_module(
        monkeypatch,
        tmp_path,
        worker_id=0,
        aggregator_amount=1,
        deduper_amount=2,
    )
    worker = _worker(module)
    calls = AckNack()
    client_id = 80
    worker._on_leader_message(
        _leader_report_packet(worker, client_id, sender_id=0, emitted_accounts=2),
        calls.ack,
        calls.nack,
    )

    def crash_commit(*_args, **_kwargs):
        raise RuntimeError("crash after leader EOF publish before INPUT_DONE")

    monkeypatch.setattr(worker._handler, "commit_done", crash_commit)
    worker._on_leader_message(
        _leader_report_packet(worker, client_id, sender_id=1, emitted_accounts=3),
        calls.ack,
        calls.nack,
    )
    assert calls.nacks == 1
    assert _gateway_eof_totals(worker, worker._gateway_eof_output) == [5]

    recovered = _worker(module)
    assert _gateway_eof_totals(recovered, recovered._gateway_eof_output) == [5]
    assert recovered._state.is_leader_closed(client_id)
