import threading
import time


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
                bank_names={bank_id: f"Bank {bank_id}"},
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
        threading.Thread(target=emit, args=(1, "001"), name="emit-1"),
        threading.Thread(target=emit, args=(2, "002"), name="emit-2"),
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
