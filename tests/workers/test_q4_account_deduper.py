import importlib
import sys

from common.message_protocol.internal.common import MessageType


def _load_module(
    monkeypatch,
    *,
    worker_id=0,
    pair_reducer_amount=1,
    deduper_amount=1,
    batch_max=5000,
):
    monkeypatch.setenv("ID", str(worker_id))
    monkeypatch.setenv("MOM_HOST", "mom")
    monkeypatch.setenv("Q4_ACCOUNT_DEDUPER_EXCHANGE", "q4_account_deduper")
    monkeypatch.setenv("Q4_ACCOUNT_DEDUPER_ROUTING_PREFIX", "q4_account_deduper")
    monkeypatch.setenv("Q4_PAIR_REDUCER_AMOUNT", str(pair_reducer_amount))
    monkeypatch.setenv("Q4_ACCOUNT_DEDUPER_AMOUNT", str(deduper_amount))
    monkeypatch.setenv(
        "Q4_ACCOUNT_DEDUPER_RESPONSE_QUEUE_PREFIX",
        "q4_account_deduper_response",
    )
    monkeypatch.setenv("GATEWAY_Q4_QUEUE", "gateway_q4_queue")
    monkeypatch.setenv("Q4_ACCOUNT_DEDUPER_BATCH_BYTES", str(1024 * 1024))
    monkeypatch.setenv("Q4_ACCOUNT_DEDUPER_BATCH_MAX_ACCOUNTS", str(batch_max))

    module_name = "workers.q4_account_deduper.account_deduper"
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

    class FakeQueue:
        instances = []

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.sent = []
            self.closed = False
            self.stop_requested = False
            FakeQueue.instances.append(self)

        @property
        def queue_name(self):
            return self.args[1]

        def send(self, message, routing_key=None):
            assert routing_key is None
            self.sent.append((routing_key, message))

        def start_consuming(self, callback):
            self.callback = callback

        def request_stop_consuming(self):
            self.stop_requested = True

        def close(self):
            self.closed = True

    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", FakeQueue)
    return module, FakeExchange, FakeQueue


def _account(module, bank, account):
    return module.Q4AccountId(bank_id=bank, account=account)


def _data_payload(module, accounts):
    return module.Q4AccountIdSerializer.serialize_batch(accounts)


def _control_payload(worker, sender_id, expected_total, processed_count=0):
    return worker._control_payload(sender_id, expected_total, processed_count)


def _gateway_accounts(worker, module, output):
    accounts = []
    for _, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.DATA:
            continue
        accounts.extend(module.Q4AccountIdSerializer.deserialize_batch(payload))
    return accounts


def _gateway_eof_totals(worker, output):
    totals = []
    for _, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.EOF:
            continue
        control = worker._control_serializer.deserialize(payload)
        totals.append(control.expected_total)
    return totals


def _leader_reports(worker, output):
    reports = []
    for _, packet in output.sent:
        msg_type, _, payload = worker._proto.unpack_packet(packet)
        if msg_type != MessageType.EOF_RECEIVED:
            continue
        reports.append(worker._control_serializer.deserialize(payload))
    return reports


def test_account_deduper_deduplicates_in_memory_and_sends_single_gateway_eof(
    monkeypatch,
):
    module, _, _ = _load_module(
        monkeypatch,
        pair_reducer_amount=1,
        deduper_amount=1,
        batch_max=2,
    )
    worker = module.Q4AccountDeduperWorker()
    output = worker._gateway_output
    client_id = 71

    a = _account(module, "1", "A")
    b = _account(module, "2", "B")

    worker._accept_accounts(client_id, _data_payload(module, [b, a, b]))
    assert worker._accounts_by_client[client_id] == {("1", "A"), ("2", "B")}
    assert output.sent == []

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=3),
    )

    assert _gateway_accounts(worker, module, output) == [a, b]
    assert _gateway_eof_totals(worker, output) == [2]
    assert client_id in worker._closed_by_client

    sent_after_close = len(output.sent)
    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=3),
    )
    worker._accept_accounts(client_id, _data_payload(module, [_account(module, "9", "L")]))
    assert len(output.sent) == sent_after_close


def test_account_deduper_waits_for_all_pair_reducer_eofs_and_reports_leader(
    monkeypatch,
):
    module, _, _ = _load_module(
        monkeypatch,
        worker_id=1,
        pair_reducer_amount=2,
        deduper_amount=3,
    )
    worker = module.Q4AccountDeduperWorker()
    gateway_output = worker._gateway_output
    leader_output = worker._leader_output
    client_id = 72

    account = _account(module, "7", "OWNER")
    worker._accept_accounts(client_id, _data_payload(module, [account, account]))

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=0, expected_total=2),
    )
    assert gateway_output.sent == []
    assert leader_output.sent == []

    worker._handle_eof(
        client_id,
        _control_payload(worker, sender_id=1, expected_total=0),
    )

    assert _gateway_accounts(worker, module, gateway_output) == [account]
    assert _gateway_eof_totals(worker, gateway_output) == []
    reports = _leader_reports(worker, leader_output)
    assert [(r.sender_id, r.expected_total) for r in reports] == [(1, 1)]


def test_account_deduper_leader_sends_one_gateway_eof_after_all_reports(
    monkeypatch,
):
    module, _, _ = _load_module(
        monkeypatch,
        worker_id=0,
        pair_reducer_amount=1,
        deduper_amount=3,
    )
    worker = module.Q4AccountDeduperWorker()
    output = worker._gateway_eof_output
    client_id = 73

    worker._handle_leader_report(
        client_id,
        module.ControlMessage(sender_id=0, expected_total=2, processed_count=0),
        output,
    )
    worker._handle_leader_report(
        client_id,
        module.ControlMessage(sender_id=1, expected_total=3, processed_count=0),
        output,
    )
    worker._handle_leader_report(
        client_id,
        module.ControlMessage(sender_id=1, expected_total=9, processed_count=0),
        output,
    )
    assert _gateway_eof_totals(worker, output) == []

    worker._handle_leader_report(
        client_id,
        module.ControlMessage(sender_id=2, expected_total=4, processed_count=0),
        output,
    )

    assert _gateway_eof_totals(worker, output) == [9]
