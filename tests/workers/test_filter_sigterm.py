"""Graceful SIGTERM shutdown for the filter worker.

The filter consumes its input queue on the main thread and the control exchange
on a second thread. This exercises the real shutdown: handle_sigterm() must stop
both consumers on their own ioloops (request_stop_consuming, never a cross-thread
stop_consuming), let start() return, and close every connection by its owner.
Applies to all filter configs (usd/q1/date/q5_format) — same code path.
"""

import threading


def _setup_filter(pika_env, monkeypatch, configuration="USD"):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("CONFIGURATION", configuration)
    monkeypatch.setenv("INPUT_QUEUE", "filter_usd_queue")
    monkeypatch.setenv("GATEWAY_QUEUE", "gateway_results_queue")
    monkeypatch.setenv("FILTER_DATE_QUEUE", "filter_date_queue")
    monkeypatch.setenv("FILTER_Q1_QUEUE", "filter_q1_queue")
    monkeypatch.setenv("SUM_Q2_QUEUE", "sum_q2_queue")
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
    return module


def test_filter_sigterm_stops_both_consumers_and_closes(pika_env, monkeypatch):
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
        # Both the main input consumer and the control-thread consumer are live.
        assert worker.input_queue.wait_started(2), "input consumer never started"
        assert worker.control_input.wait_started(2), "control consumer never started"

        worker.handle_sigterm()

        assert done.wait(timeout=10), "filter start() did not return after SIGTERM"
    finally:
        worker.handle_sigterm()
        runner.join(timeout=5)

    assert not runner.is_alive()
    # Both consumers stopped via request_stop_consuming (no cross-thread stop).
    assert worker.input_queue.stop_calls == 1
    assert worker.control_input.stop_calls == 1
    # Every connection closed by its owning thread.
    assert worker.input_queue.closed
    assert worker.control_input.closed
    assert worker.control_output.closed
    assert all(q.closed for q in worker.output_queues.values())


def test_filter_handle_sigterm_is_idempotent(pika_env, monkeypatch):
    module = _setup_filter(pika_env, monkeypatch)
    worker = module.FilterWorker()

    done = threading.Event()
    threading.Thread(target=lambda: (worker.start(), done.set()), name="filter-start").start()

    assert worker.input_queue.wait_started(2)
    assert worker.control_input.wait_started(2)

    worker.handle_sigterm()
    worker.handle_sigterm()  # duplicate signal must be a no-op

    assert done.wait(timeout=10)
    assert worker.input_queue.stop_calls == 1
    assert worker.control_input.stop_calls == 1
