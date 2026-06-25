"""Graceful SIGTERM shutdown for the filter_q5_usd worker.

Same shape as the filter: main input consumer + control and response threads.
handle_sigterm() must stop all three on their own ioloops (no cross-thread
stop_consuming), let start() return, and close every connection by its owner.
"""

import threading


def _setup(pika_env, monkeypatch):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("INPUT_EXCHANGE", "filter_q5_usd_exchange")
    monkeypatch.setenv("INPUT_QUEUE", "filter_q5_usd_0")
    monkeypatch.setenv("INPUT_ROUTING_PREFIX", "filter_q5_usd")
    monkeypatch.setenv("AGGREGATION_AMOUNT", "1")
    monkeypatch.setenv("AGGREGATION_PREFIX", "aggregation_q5")
    monkeypatch.setenv("FILTER_Q5_USD_AMOUNT", "1")

    module = pika_env.import_fresh("workers.filter_q5_usd.filter_q5_usd")

    def factory(*args, **kwargs):
        return pika_env.BlockingFakeConsumer(block_timeout=30)

    # The module imports these names directly, so patch them on the module.
    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", factory)
    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", factory)
    monkeypatch.setattr(module, "LazyQueue", factory)
    return module


def test_filter_q5_usd_sigterm_stops_all_consumers_and_closes(pika_env, monkeypatch):
    module = _setup(pika_env, monkeypatch)
    worker = module.FilterQ5UsdWorker()

    done = threading.Event()

    def run():
        try:
            worker.start()
        finally:
            done.set()

    runner = threading.Thread(target=run, name="q5usd-start")
    runner.start()
    try:
        assert worker.input_queue.wait_started(2), "input consumer never started"
        # control_consumer / response_consumer are created inside their threads.
        deadline = threading.Event()
        for _ in range(200):
            if worker.control_consumer is not None and worker.response_consumer is not None:
                break
            deadline.wait(0.01)
        assert worker.control_consumer is not None and worker.control_consumer.wait_started(2)
        assert worker.response_consumer is not None and worker.response_consumer.wait_started(2)

        worker.handle_sigterm()
        assert done.wait(timeout=10), "start() did not return after SIGTERM"
    finally:
        worker.handle_sigterm()
        runner.join(timeout=5)

    assert not runner.is_alive()
    assert worker.input_queue.stop_calls == 1
    assert worker.control_consumer.stop_calls == 1
    assert worker.response_consumer.stop_calls == 1
    # Closed by their owning threads.
    assert worker.input_queue.closed          # close() (main)
    assert worker.control_consumer.closed     # control thread finally
    assert worker.response_consumer.closed    # response thread finally
    assert all(q.closed for q in worker._main_control_senders.values())


def test_filter_q5_usd_handle_sigterm_sets_rpc_cancel(pika_env, monkeypatch):
    module = _setup(pika_env, monkeypatch)
    worker = module.FilterQ5UsdWorker()

    assert not worker._shutdown.is_set()
    worker.handle_sigterm()
    # The rates RPC observes this event and aborts at its next poll slice.
    assert worker._shutdown.is_set()
