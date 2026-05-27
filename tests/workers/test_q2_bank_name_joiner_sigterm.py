"""Graceful SIGTERM shutdown for the q2_bank_name_joiner worker.

Two consumers (q2 + accounts) each on their own daemon thread; the main thread
blocks until they finish. stop() must stop both on their own ioloops (no
cross-thread stop_consuming), the bounded join must let start() return, and
every connection must be closed by its owner.
"""

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

    def factory(*args, **kwargs):
        return pika_env.BlockingFakeConsumer(block_timeout=30)

    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", factory)

    config = module.BankNameJoinerConfig(
        id=0,
        mom_host="rabbitmq",
        q2_input_queue="q2_enrich_queue",
        accounts_input_queue="accounts_line_batch_queue",
        output_queue="join_q2_results_queue",
    )
    return module, module.BankNameJoinerWorker(config)


def test_q2_joiner_sigterm_stops_both_consumers_and_closes(pika_env, monkeypatch):
    _module, worker = _make_worker(pika_env, monkeypatch)

    done = threading.Event()

    def run():
        try:
            worker.start()   # blocks in _await_consumers until stop()
            worker.close()   # mirrors main()'s finally
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
    # Each consumer closed by its own thread's finally; output by close() (main).
    assert q2.closed
    assert accounts.closed
    assert worker._output_queue.closed


def test_q2_joiner_stop_is_idempotent(pika_env, monkeypatch):
    _module, worker = _make_worker(pika_env, monkeypatch)

    done = threading.Event()
    threading.Thread(target=lambda: (worker.start(), done.set()), name="q2joiner-start").start()

    q2 = _wait_attr(lambda: worker._q2_consumer)
    accounts = _wait_attr(lambda: worker._accounts_consumer)
    assert q2.wait_started(2)
    assert accounts.wait_started(2)

    worker.stop()
    worker.stop()  # duplicate signal must be a no-op

    assert done.wait(timeout=10)
    assert q2.stop_calls == 1
    assert accounts.stop_calls == 1
