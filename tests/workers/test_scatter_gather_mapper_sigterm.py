"""Graceful SIGTERM shutdown for the scatter_gather mapper.

Single input consumer on the main thread; control and response consumers run on
their own threads only when SG_MAPPER_AMOUNT > 1. handle_sigterm() must stop
whatever is running on its own ioloop (no cross-thread stop_consuming), let
start() return, and close every connection by its owner.
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


def _setup(pika_env, monkeypatch, amount):
    monkeypatch.setenv("ID", "0")
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("INPUT_QUEUE", "sg_mapper_queue")
    monkeypatch.setenv("SG_LINKER_EXCHANGE", "sg_linker_exchange")
    monkeypatch.setenv("SG_LINKER_AMOUNT", "1")
    monkeypatch.setenv("SG_MAPPER_AMOUNT", str(amount))

    module = pika_env.import_fresh("workers.scatter_gather.mapper.mapper")

    def factory(*args, **kwargs):
        return pika_env.BlockingFakeConsumer(block_timeout=30)

    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", factory)
    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", factory)
    return module


def _run(worker):
    done = threading.Event()
    threading.Thread(
        target=lambda: (worker.start(), done.set()), name="mapper-start"
    ).start()
    return done


def test_mapper_sigterm_multithreaded_stops_all_and_closes(pika_env, monkeypatch):
    module = _setup(pika_env, monkeypatch, amount=2)
    worker = module.ScatterGatherMapper()

    done = _run(worker)
    try:
        assert worker._input.wait_started(2), "input never started"
        control = _wait_attr(lambda: worker._control_consumer)
        response = _wait_attr(lambda: worker._response_consumer)
        assert control.wait_started(2), "control never started"
        assert response.wait_started(2), "response never started"

        worker.handle_sigterm()
        assert done.wait(timeout=10), "start() did not return after SIGTERM"
    finally:
        worker.handle_sigterm()

    assert worker._input.stop_calls == 1
    assert control.stop_calls == 1
    assert response.stop_calls == 1
    assert worker._input.closed           # close() (main)
    assert worker._control_sender.closed  # close() (main)
    assert all(linker.closed for linker in worker._linkers)
    assert control.closed                 # control thread finally
    assert response.closed                # response thread finally


def test_mapper_sigterm_singlethreaded_stops_input(pika_env, monkeypatch):
    module = _setup(pika_env, monkeypatch, amount=1)
    worker = module.ScatterGatherMapper()

    assert worker._control_thread is None
    assert worker._response_thread is None
    assert worker._control_sender is None

    done = _run(worker)
    try:
        assert worker._input.wait_started(2), "input never started"
        worker.handle_sigterm()
        assert done.wait(timeout=10), "start() did not return after SIGTERM"
    finally:
        worker.handle_sigterm()

    assert worker._input.stop_calls == 1
    assert worker._input.closed
    assert all(linker.closed for linker in worker._linkers)


def test_mapper_handle_sigterm_is_idempotent(pika_env, monkeypatch):
    module = _setup(pika_env, monkeypatch, amount=2)
    worker = module.ScatterGatherMapper()

    done = _run(worker)
    assert worker._input.wait_started(2)
    control = _wait_attr(lambda: worker._control_consumer)
    response = _wait_attr(lambda: worker._response_consumer)
    assert control.wait_started(2)
    assert response.wait_started(2)

    worker.handle_sigterm()
    worker.handle_sigterm()  # duplicate must be a no-op

    assert done.wait(timeout=10)
    assert worker._input.stop_calls == 1
    assert control.stop_calls == 1
    assert response.stop_calls == 1
