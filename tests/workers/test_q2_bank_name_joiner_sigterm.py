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


def _make_worker(pika_env, monkeypatch):
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
    )
    return module, module.BankNameJoinerWorker(config), created


def test_q2_joiner_sigterm_stops_both_consumers_and_closes(pika_env, monkeypatch):
    _module, worker, created = _make_worker(pika_env, monkeypatch)

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
    assert not created.get("join_q2_results_queue")


def test_q2_joiner_stop_is_idempotent(pika_env, monkeypatch):
    _module, worker, _created = _make_worker(pika_env, monkeypatch)

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


def test_q2_joiner_output_connections_are_per_thread(pika_env, monkeypatch):
    module, worker, created = _make_worker(pika_env, monkeypatch)

    outputs = {}
    outputs_lock = threading.Lock()
    start_barrier = threading.Barrier(2)
    close_barrier = threading.Barrier(2)
    errors = []

    def emit(client_id, bank_id):
        try:
            start_barrier.wait(timeout=2)
            state = module.ClientState(
                bank_names={bank_id: [f"Bank {bank_id}"]},
                q2_results={
                    bank_id: module.Q2BankMaxPartial(
                        bank_id=bank_id,
                        from_account=f"account-{client_id}",
                        amount=100.0 + client_id,
                    )
                },
            )
            with worker._lock:
                worker._states[client_id] = state

            worker._emit_results(client_id)
            thread_id = threading.get_ident()
            with worker._output_queues_lock:
                output = worker._output_queues_by_thread[thread_id]
            with outputs_lock:
                outputs[threading.current_thread().name] = output
            close_barrier.wait(timeout=2)
        except Exception as exc:
            errors.append(exc)
        finally:
            worker._close_thread_output_queue()

    threads = [
        threading.Thread(target=emit, args=(1, "1"), name="emit-1"),
        threading.Thread(target=emit, args=(2, "2"), name="emit-2"),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    assert len(outputs) == 2
    assert len({id(output) for output in outputs.values()}) == 2
    assert all(output.closed for output in outputs.values())
    assert len(created["join_q2_results_queue"]) == 2
    assert all(len(output.sent) == 2 for output in outputs.values())


def test_q2_joiner_accounts_parse_uses_notebook_bank_id_coercion(
    pika_env, monkeypatch
):
    module, worker, _created = _make_worker(pika_env, monkeypatch)

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


def test_q2_joiner_consumes_addressed_inputs(pika_env, monkeypatch):
    module, worker, created = _make_worker(pika_env, monkeypatch)
    client_id = 77
    proto = module.InternalProtocol()
    calls = {"acks": 0, "nacks": 0}

    def ack():
        calls["acks"] += 1

    def nack():
        calls["nacks"] += 1

    q2_payload = module.Q2BankMaxPartialSerializer.serialize(
        module.Q2BankMaxPartial(
            bank_id="001",
            from_account="acc-1",
            amount=10.0,
        )
    )
    q2_data = proto.create_addressed_packet(
        msg_type=module.MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=3,
        seq=0,
        payload=q2_payload,
    )
    q2_eof = proto.create_addressed_packet(
        msg_type=module.MessageType.EOF,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=3,
        seq=1,
        payload=module.ControlMessageSerializer().serialize(
            module.ControlMessage(sender_id=3, expected_total=1, processed_count=0)
        ),
    )
    accounts_payload = module.LineBatchSerializer.serialize(
        LineBatch(
            file_type=module.FILE_TYPE_ACCOUNTS,
            rel_path="accounts.csv",
            batch_id=0,
            first_line_number=2,
            header=("Bank ID", "Bank Name"),
            lines=(b"001,Raw One",),
        )
    )
    accounts_data = proto.create_addressed_packet(
        msg_type=module.MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=5,
        seq=0,
        payload=accounts_payload,
    )
    accounts_eof = proto.create_addressed_packet(
        msg_type=module.MessageType.EOF,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=5,
        seq=1,
        payload=module.ControlMessageSerializer().serialize(
            module.ControlMessage(sender_id=5, expected_total=1, processed_count=0)
        ),
    )

    worker._on_q2_message(q2_data, ack, nack)
    worker._on_q2_message(q2_eof, ack, nack)
    worker._on_accounts_message(accounts_data, ack, nack)
    worker._on_accounts_message(accounts_eof, ack, nack)

    assert calls == {"acks": 4, "nacks": 0}
    output = created["join_q2_results_queue"][0]
    data_packet, eof_packet = output.sent

    msg_type, packet_client_id, payload = worker._protocol.unpack_packet(data_packet)
    assert msg_type == module.MessageType.DATA
    assert packet_client_id == client_id
    result = module.Q2BankMaxResultSerializer.deserialize(payload)
    assert (result.bank_id, result.from_account, result.bank_name, result.amount) == (
        "1",
        "acc-1",
        "Raw One",
        10.0,
    )

    msg_type, packet_client_id, payload = worker._protocol.unpack_packet(eof_packet)
    assert msg_type == module.MessageType.EOF
    assert packet_client_id == client_id
    control = worker._control_serializer.deserialize(payload)
    assert control.expected_total == 1


def test_q2_joiner_emits_only_banks_matched_by_notebook_inner_join(
    pika_env, monkeypatch
):
    module, worker, created = _make_worker(pika_env, monkeypatch)
    client_id = 77
    with worker._lock:
        worker._states[client_id] = module.ClientState(
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

    worker._emit_results(client_id)

    output = created["join_q2_results_queue"][0]
    data_packets = output.sent[:-1]
    eof_packet = output.sent[-1]
    rows = set()
    for packet in data_packets:
        msg_type, packet_client_id, payload = worker._protocol.unpack_packet(packet)
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

    msg_type, packet_client_id, payload = worker._protocol.unpack_packet(eof_packet)
    assert msg_type == module.MessageType.EOF
    assert packet_client_id == client_id
    control = worker._control_serializer.deserialize(payload)
    assert control.expected_total == 3
