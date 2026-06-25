"""Graceful SIGTERM shutdown for the sum worker.

Same shape as filter_q5_usd: main input consumer + control and response
threads. handle_sigterm() must stop all three on their own ioloops (no
cross-thread stop_consuming), let start() return, and close every connection
by its owner.
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


def _setup(pika_env, monkeypatch):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("INPUT_QUEUE", "sum_0")
    monkeypatch.setenv("INPUT_EXCHANGE", "sum_exchange")
    monkeypatch.setenv("INPUT_ROUTING_PREFIX", "sum")
    monkeypatch.setenv("CONFIGURATION", "Q2")
    monkeypatch.setenv("SUM_AMOUNT", "1")
    monkeypatch.setenv("SUM_PREFIX", "sum")
    monkeypatch.setenv("AGGREGATION_AMOUNT", "1")
    monkeypatch.setenv("AGGREGATION_PREFIX", "aggregation")

    module = pika_env.import_fresh("workers.sum.sums")

    def factory(*args, **kwargs):
        return pika_env.BlockingFakeConsumer(block_timeout=30)

    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", factory)
    monkeypatch.setattr(module.middleware, "MessageMiddlewareExchangeRabbitMQ", factory)
    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", factory)
    monkeypatch.setattr(module, "LazyQueue", factory)
    return module


def test_sum_sigterm_stops_all_consumers_and_closes(pika_env, monkeypatch):
    module = _setup(pika_env, monkeypatch)
    worker = module.SumWorker()

    done = threading.Event()

    def run():
        try:
            worker.start()
        finally:
            done.set()

    runner = threading.Thread(target=run, name="sum-start")
    runner.start()
    try:
        input_consumer = _wait_attr(lambda: worker._input_queue)
        assert input_consumer.wait_started(2), "input consumer never started"
        control = _wait_attr(lambda: worker._control_consumer)
        response = _wait_attr(lambda: worker._response_consumer)
        assert control.wait_started(2), "control consumer never started"
        assert response.wait_started(2), "response consumer never started"

        worker.handle_sigterm()
        assert done.wait(timeout=10), "start() did not return after SIGTERM"
    finally:
        worker.handle_sigterm()
        runner.join(timeout=5)

    assert not runner.is_alive()
    assert input_consumer.stop_calls == 1
    assert control.stop_calls == 1
    assert response.stop_calls == 1
    # Each connection closed by its owning thread.
    assert input_consumer.closed              # close() (main)
    assert control.closed                     # control thread finally
    assert response.closed                    # response thread finally
    # Output/control senders are now thread-local, created lazily on first send;
    # with no message flow there is nothing to close.


def test_sum_handle_sigterm_is_idempotent(pika_env, monkeypatch):
    module = _setup(pika_env, monkeypatch)
    worker = module.SumWorker()

    done = threading.Event()
    runner = threading.Thread(
        target=lambda: (worker.start(), done.set()),
        name="sum-start",
    )
    runner.start()

    input_consumer = _wait_attr(lambda: worker._input_queue)
    assert input_consumer.wait_started(2)
    control = _wait_attr(lambda: worker._control_consumer)
    response = _wait_attr(lambda: worker._response_consumer)
    assert control.wait_started(2)
    assert response.wait_started(2)

    worker.handle_sigterm()
    worker.handle_sigterm()  # duplicate signal must be a no-op

    assert done.wait(timeout=10)
    runner.join(timeout=5)
    assert input_consumer.stop_calls == 1
    assert control.stop_calls == 1
    assert response.stop_calls == 1
