"""Graceful SIGTERM shutdown for the filter worker.

The filter consumes its input queue on the main thread, a control queue on a
second thread, and a response queue on a third thread.  This exercises the real
shutdown: handle_sigterm() must stop all three consumers on their own ioloops
(request_stop_consuming, never a cross-thread stop_consuming), let start()
return, and close every connection by its owner.
Applies to all filter configs (usd/q1/date/q5_format) — same code path.
"""

import threading
import time


def _wait_consumer(attr, worker, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if getattr(worker, attr) is not None:
            return getattr(worker, attr)
        time.sleep(0.01)
    raise AssertionError(f"{attr} consumer was never created")


def _setup_filter(pika_env, monkeypatch, configuration="USD"):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("CONFIGURATION", configuration)
    monkeypatch.setenv("INPUT_QUEUE", "filter_usd_queue")
    monkeypatch.setenv("GATEWAY_QUEUE", "gateway_results_queue")
    monkeypatch.setenv("FILTER_DATE_QUEUE", "filter_date_queue")
    monkeypatch.setenv("FILTER_Q1_QUEUE", "filter_q1_queue")
    monkeypatch.setenv("SUM_Q2_EXCHANGE", "sum_q2_exchange")
    monkeypatch.setenv("SUM_Q2_ROUTING_PREFIX", "sum_q2")
    monkeypatch.setenv("SUM_Q2_AMOUNT", "1")
    monkeypatch.setenv("FILTER_Q3_QUEUE", "filter_q3_queue")
    monkeypatch.setenv("SCATTER_GATHER_MAPPER_QUEUE", "sg_mapper_queue")
    monkeypatch.setenv("FILTER_Q5_USD_QUEUE", "filter_q5_usd_queue")
    monkeypatch.setenv("SUM_PREFIX", "sum_q3")
    monkeypatch.setenv("SUM_Q3_QUEUE", "sum_q3_queue")
    monkeypatch.setenv("FILTER_AMOUNT", "1")
    monkeypatch.setenv("FILTER_PREFIX", "filter")
    monkeypatch.setenv("USD_ENABLE_Q1", "0")
    monkeypatch.setenv("USD_ENABLE_Q2", "1")
    monkeypatch.setenv("USD_ENABLE_DATE", "0")
    monkeypatch.setenv("DATE_ENABLE_Q3", "0")
    monkeypatch.setenv("DATE_ENABLE_Q4", "0")

    module = pika_env.import_fresh("workers.filter.filters")

    # Every middleware endpoint becomes a blocking fake: consumers block in
    # start_consuming until request_stop_consuming, publishers are no-ops.
    def factory(*args, **kwargs):
        return pika_env.BlockingFakeConsumer(block_timeout=30)

    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", factory)
    monkeypatch.setattr(module.middleware, "MessageMiddlewareExchangeRabbitMQ", factory)
    monkeypatch.setattr(module.middleware, "ShardedByClientPublisher", factory)
    monkeypatch.setattr(module.middleware, "ShardedPublisher", factory)
    return module


def test_filter_sigterm_stops_all_consumers_and_closes(pika_env, monkeypatch):
    module = _setup_filter(pika_env, monkeypatch)
    worker = module.FilterWorker()

    done = threading.Event()

    def run():
        try:
            worker.start()
        finally:
            done.set()

    runner = threading.Thread(target=run, name="filter-start")
    runner.start()
    try:
        # All three consumers (input, control, response) must start.
        assert worker.input_queue.wait_started(2), "input consumer never started"
        control_consumer = _wait_consumer("_control_consumer", worker)
        assert control_consumer.wait_started(2), "control consumer never started"
        response_consumer = _wait_consumer("_response_consumer", worker)
        assert response_consumer.wait_started(2), "response consumer never started"

        worker.handle_sigterm()

        assert done.wait(timeout=10), "filter start() did not return after SIGTERM"
    finally:
        worker.handle_sigterm()
        runner.join(timeout=5)

    assert not runner.is_alive()
    # All consumers stopped via request_stop_consuming (no cross-thread stop).
    assert worker.input_queue.stop_calls == 1
    assert control_consumer.stop_calls == 1
    assert response_consumer.stop_calls == 1
    # Connections closed by their owning threads.
    assert worker.input_queue.closed
    assert control_consumer.closed
    assert response_consumer.closed
    assert all(q.closed for q in worker.output_queues.values())


def test_filter_handle_sigterm_is_idempotent(pika_env, monkeypatch):
    module = _setup_filter(pika_env, monkeypatch)
    worker = module.FilterWorker()

    done = threading.Event()
    threading.Thread(
        target=lambda: (worker.start(), done.set()), name="filter-start"
    ).start()

    assert worker.input_queue.wait_started(2)
    control_consumer = _wait_consumer("_control_consumer", worker)
    assert control_consumer.wait_started(2)
    response_consumer = _wait_consumer("_response_consumer", worker)
    assert response_consumer.wait_started(2)

    worker.handle_sigterm()
    worker.handle_sigterm()  # duplicate signal must be a no-op

    assert done.wait(timeout=10)
    assert worker.input_queue.stop_calls == 1
    assert control_consumer.stop_calls == 1
    assert response_consumer.stop_calls == 1
