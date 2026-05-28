"""Graceful SIGTERM shutdown for the q3_barrier worker.

Main thread consumes the averages input; a second thread consumes the
candidates input. handle_sigterm() must stop both on their own ioloops (no
cross-thread stop_consuming), let start() return, and close every connection
by its owner — averages_input/averages_output on the main thread, candidates
input + its output on the candidates thread.
"""

import threading
import time


def _setup(pika_env, monkeypatch):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("Q3_AVERAGES_QUEUE", "q3_averages_queue")
    monkeypatch.setenv("Q3_CANDIDATES_QUEUE", "q3_candidates_queue")
    monkeypatch.setenv("GATEWAY_Q3_QUEUE", "gateway_q3_queue")
    monkeypatch.setenv("Q3_BARRIER_AMOUNT", "1")  # no sharding -> plain queues

    module = pika_env.import_fresh("workers.q3_barrier.q3_barrier")

    def factory(*args, **kwargs):
        return pika_env.BlockingFakeConsumer(block_timeout=30)

    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", factory)
    monkeypatch.setattr(module.middleware, "MessageMiddlewareExchangeRabbitMQ", factory)
    return module


def test_q3_barrier_sigterm_stops_both_consumers_and_closes(pika_env, monkeypatch):
    module = _setup(pika_env, monkeypatch)
    worker = module.Q3BarrierWorker()

    done = threading.Event()

    def run():
        try:
            worker.start()
        finally:
            done.set()

    runner = threading.Thread(target=run, name="q3barrier-start")
    runner.start()
    try:
        assert worker.averages_input.wait_started(2), "averages consumer never started"
        assert worker.candidates_input.wait_started(2), "candidates consumer never started"

        worker.handle_sigterm()
        assert done.wait(timeout=10), "start() did not return after SIGTERM"
    finally:
        worker.handle_sigterm()
        runner.join(timeout=5)

    assert not runner.is_alive()
    assert worker.averages_input.stop_calls == 1
    assert worker.candidates_input.stop_calls == 1
    # Each connection closed by its owning thread.
    assert worker.averages_input.closed          # close() (main)
    assert worker.averages_output_queue.closed    # close() (main)
    assert worker.candidates_input.closed         # candidates thread finally


def test_q3_barrier_handle_sigterm_is_idempotent(pika_env, monkeypatch):
    module = _setup(pika_env, monkeypatch)
    worker = module.Q3BarrierWorker()

    done = threading.Event()
    threading.Thread(target=lambda: (worker.start(), done.set()), name="q3barrier-start").start()

    assert worker.averages_input.wait_started(2)
    assert worker.candidates_input.wait_started(2)

    worker.handle_sigterm()
    worker.handle_sigterm()  # duplicate signal must be a no-op

    assert done.wait(timeout=10)
    assert worker.averages_input.stop_calls == 1
    assert worker.candidates_input.stop_calls == 1
