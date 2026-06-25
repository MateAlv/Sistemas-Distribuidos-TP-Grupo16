import threading
import time

from common.message_protocol.internal.line_batch_serializer import LineBatch


def _wait_attr(get, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = get()
        if value is not None:
            return value
        time.sleep(0.01)
    raise AssertionError("attribute was never set")


def _make_worker(pika_env, monkeypatch, tmp_path):
    module = pika_env.import_fresh("workers.q2_bank_name_joiner.bank_name_joiner")
    created = {}

    def factory(*args, **kwargs):
        consumer = pika_env.BlockingFakeConsumer(block_timeout=30)
        if len(args) >= 2:
            created.setdefault(args[1], []).append(consumer)
        return consumer

    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", factory)

    config = module.BankNameJoinerConfig(
        id=0,
        mom_host="rabbitmq",
        q2_input_queue="q2_enrich_queue",
        accounts_input_queue="accounts_line_batch_queue",
        output_queue="join_q2_results_queue",
        state_dir=str(tmp_path / "q2_bank_name_joiner_state"),
        snapshot_interval=1000,
    )
    return module, module.BankNameJoinerWorker(config), created


def _calls():
    calls = {"acks": 0, "nacks": 0}

    def ack():
        calls["acks"] += 1

    def nack(*_args, **_kwargs):
        calls["nacks"] += 1

    return calls, ack, nack


def _q2_data(module, client_id: int, sender_id: int = 3, seq: int = 0) -> bytes:
    payload = module.Q2BankMaxPartialSerializer.serialize(
        module.Q2BankMaxPartial(
            bank_id="001",
            from_account="acc-1",
            amount=10.0,
        )
    )
    return module.InternalProtocol().create_addressed_packet(
        msg_type=module.MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=sender_id,
        seq=seq,
        payload=payload,
    )


def _eof(module, client_id: int, sender_id: int, seq: int, expected: int) -> bytes:
    payload = module.ControlMessageSerializer().serialize(
        module.ControlMessage(
            sender_id=sender_id,
            expected_total=expected,
            processed_count=0,
        )
    )
    return module.InternalProtocol().create_addressed_packet(
        msg_type=module.MessageType.EOF,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=sender_id,
        seq=seq,
        payload=payload,
    )


def _accounts_data(module, client_id: int, sender_id: int = 5, seq: int = 0) -> bytes:
    payload = module.LineBatchSerializer.serialize(
        LineBatch(
            file_type=module.FILE_TYPE_ACCOUNTS,
            rel_path="accounts.csv",
            batch_id=0,
            first_line_number=2,
            header=("Bank ID", "Bank Name"),
            lines=(b"001,Raw One",),
        )
    )
    return module.InternalProtocol().create_addressed_packet(
        msg_type=module.MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=sender_id,
        seq=seq,
        payload=payload,
    )


def test_q2_joiner_sigterm_stops_both_consumers_and_closes(
    pika_env, monkeypatch, tmp_path
):
    _module, worker, created = _make_worker(pika_env, monkeypatch, tmp_path)

    done = threading.Event()

    def run():
        try:
            worker.start()
            worker.close()
        finally:
            done.set()

    runner = threading.Thread(target=run, name="q2joiner-start")
    runner.start()
    try:
        q2 = _wait_attr(lambda: worker._q2_consumer)
        accounts = _wait_attr(lambda: worker._accounts_consumer)
        assert q2.wait_started(2), "q2 consumer never started"
        assert accounts.wait_started(2), "accounts consumer never started"

        worker.stop()
        assert done.wait(timeout=10), "start() did not return after stop()"
    finally:
        worker.stop()
        runner.join(timeout=5)

    assert not runner.is_alive(), "worker thread still alive after shutdown"
    assert q2.stop_calls == 1
    assert accounts.stop_calls == 1
    assert q2.closed
    assert accounts.closed
    assert len(created["join_q2_results_queue"]) == 1
    assert created["join_q2_results_queue"][0].closed


def test_q2_joiner_stop_is_idempotent(pika_env, monkeypatch, tmp_path):
    _module, worker, _created = _make_worker(pika_env, monkeypatch, tmp_path)

    done = threading.Event()
    threading.Thread(
        target=lambda: (worker.start(), done.set()),
        name="q2joiner-start",
    ).start()

    q2 = _wait_attr(lambda: worker._q2_consumer)
    accounts = _wait_attr(lambda: worker._accounts_consumer)
    assert q2.wait_started(2)
    assert accounts.wait_started(2)

    worker.stop()
    worker.stop()

    assert done.wait(timeout=10)
    assert q2.stop_calls == 1
    assert accounts.stop_calls == 1


def test_q2_joiner_output_connection_is_shared_and_closed(
    pika_env, monkeypatch, tmp_path
):
    _module, worker, created = _make_worker(pika_env, monkeypatch, tmp_path)

    assert len(created["join_q2_results_queue"]) == 1
    output = created["join_q2_results_queue"][0]
    assert not output.closed

    worker.close()

    assert output.closed


def test_q2_joiner_accounts_parse_uses_notebook_bank_id_coercion(
    pika_env, monkeypatch, tmp_path
):
    module, worker, _created = _make_worker(pika_env, monkeypatch, tmp_path)

    payload = module.LineBatchSerializer.serialize(
        LineBatch(
            file_type=module.FILE_TYPE_ACCOUNTS,
            rel_path="accounts.csv",
            batch_id=1,
            first_line_number=2,
            header=("Bank ID", "Bank Name"),
            lines=(b"001,Raw One", b"02,Raw Two", b"BANK_X,Named X"),
        )
    )

    assert worker._parse_accounts_batch(payload) == [
        ("1", "Raw One"),
        ("2", "Raw Two"),
        ("BANK_X", "Named X"),
    ]


def test_q2_joiner_consumes_addressed_inputs(pika_env, monkeypatch, tmp_path):
    module, worker, created = _make_worker(pika_env, monkeypatch, tmp_path)
    client_id = 77
    calls, ack, nack = _calls()

    worker._on_q2_message(_q2_data(module, client_id), ack, nack)
    worker._on_q2_message(_eof(module, client_id, sender_id=3, seq=1, expected=1), ack, nack)
    worker._on_accounts_message(_accounts_data(module, client_id), ack, nack)
    worker._on_accounts_message(
        _eof(module, client_id, sender_id=5, seq=1, expected=1), ack, nack
    )

    assert calls == {"acks": 4, "nacks": 0}
    output = created["join_q2_results_queue"][0]
    data_packet, eof_packet = output.sent

    msg_type, packet_client_id, sender_id, seq, payload = (
        worker._protocol.unpack_addressed_packet(data_packet)
    )
    assert msg_type == module.MessageType.DATA
    assert packet_client_id == client_id
    assert sender_id == 0
    assert seq == 0
    result = module.Q2BankMaxResultSerializer.deserialize(payload)
    assert (result.bank_id, result.from_account, result.bank_name, result.amount) == (
        "1",
        "acc-1",
        "Raw One",
        10.0,
    )

    msg_type, packet_client_id, sender_id, seq, payload = (
        worker._protocol.unpack_addressed_packet(eof_packet)
    )
    assert msg_type == module.MessageType.EOF
    assert packet_client_id == client_id
    assert sender_id == 0
    assert seq == 1
    control = worker._control_serializer.deserialize(payload)
    assert control.expected_total == 1


def test_q2_joiner_emits_only_banks_matched_by_notebook_inner_join(
    pika_env, monkeypatch, tmp_path
):
    module, worker, _created = _make_worker(pika_env, monkeypatch, tmp_path)
    client_id = 77
    state = module.ClientState(
        bank_names={
            "1": ["Raw One"],
            "2": ["Two A", "Two B"],
        },
        q2_results={
            "1": module.Q2BankMaxPartial(
                bank_id="1",
                from_account="acc-1",
                amount=10.0,
            ),
            "2": module.Q2BankMaxPartial(
                bank_id="2",
                from_account="acc-2",
                amount=20.0,
            ),
            "3": module.Q2BankMaxPartial(
                bank_id="3",
                from_account="missing",
                amount=30.0,
            ),
        },
    )

    outputs = worker._build_result_outputs(state, client_id)
    data_packets = [body for _dest, body in outputs[:-1]]
    eof_packet = outputs[-1][1]
    rows = set()
    for packet in data_packets:
        msg_type, packet_client_id, _sender_id, _seq, payload = (
            worker._protocol.unpack_addressed_packet(packet)
        )
        assert msg_type == module.MessageType.DATA
        assert packet_client_id == client_id
        result = module.Q2BankMaxResultSerializer.deserialize(payload)
        rows.add(
            (
                result.bank_id,
                result.from_account,
                result.bank_name,
                result.amount,
            )
        )

    assert rows == {
        ("1", "acc-1", "Raw One", 10.0),
        ("2", "acc-2", "Two A", 20.0),
        ("2", "acc-2", "Two B", 20.0),
    }

    msg_type, packet_client_id, _sender_id, seq, payload = (
        worker._protocol.unpack_addressed_packet(eof_packet)
    )
    assert msg_type == module.MessageType.EOF
    assert packet_client_id == client_id
    assert seq == 3
    control = worker._control_serializer.deserialize(payload)
    assert control.expected_total == 3


def test_q2_joiner_recovery_republishes_ready_outbox_after_publish_crash(
    pika_env, monkeypatch, tmp_path
):
    module, worker, _created = _make_worker(pika_env, monkeypatch, tmp_path)
    client_id = 77
    calls, ack, nack = _calls()

    worker._on_q2_message(_q2_data(module, client_id), ack, nack)
    worker._on_q2_message(_eof(module, client_id, sender_id=3, seq=1, expected=1), ack, nack)
    worker._on_accounts_message(_accounts_data(module, client_id), ack, nack)

    def crash_send(_body):
        raise RuntimeError("crash after INPUT_APPLIED before publish")

    worker._send_output = crash_send
    worker._on_accounts_message(
        _eof(module, client_id, sender_id=5, seq=1, expected=1), ack, nack
    )
    assert calls == {"acks": 3, "nacks": 1}
    worker._handler.wal.close()

    _module, recovered, recovered_created = _make_worker(pika_env, monkeypatch, tmp_path)

    output = recovered_created["join_q2_results_queue"][0]
    assert len(output.sent) == 2
    data_packet, eof_packet = output.sent
    msg_type, packet_client_id, _sender_id, _seq, payload = (
        recovered._protocol.unpack_addressed_packet(data_packet)
    )
    assert msg_type == module.MessageType.DATA
    assert packet_client_id == client_id
    result = module.Q2BankMaxResultSerializer.deserialize(payload)
    assert result.bank_name == "Raw One"

    msg_type, packet_client_id, _sender_id, _seq, payload = (
        recovered._protocol.unpack_addressed_packet(eof_packet)
    )
    assert msg_type == module.MessageType.EOF
    assert packet_client_id == client_id
    control = recovered._control_serializer.deserialize(payload)
    assert control.expected_total == 1
