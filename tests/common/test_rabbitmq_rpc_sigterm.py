"""SIGTERM safety of the RabbitMQ RPC middleware.

These exercise the *real* middleware against fake pika primitives to prove the
shutdown path never touches a channel directly — it schedules the stop on the
connection's ioloop via ``add_callback_threadsafe``. This is the primitive
every multi-threaded worker's graceful shutdown relies on.
"""

import threading

import pytest


def test_rpc_server_stop_schedules_stop_consuming_threadsafe(pika_env):
    channel = pika_env.FakeChannel()
    connection = pika_env.FakeConnection(channel)
    module = pika_env.import_fresh(
        "src.common.middleware.middleware_rabbitmq", connection=connection
    )

    server = module.MessageMiddlewareRpcServerRabbitMQ("rabbitmq", "rates_requests")
    server._consuming = True

    server.stop()

    # Stop is scheduled, not executed inline: the channel is untouched until the
    # owning thread's ioloop drains the callback.
    assert len(connection.callbacks) == 1
    assert channel.stop_consuming_calls == 0

    connection.callbacks[0]()
    assert channel.stop_consuming_calls == 1

    # Idempotent: a second stop schedules nothing new.
    server.stop()
    assert len(connection.callbacks) == 1


def test_rpc_server_stop_before_start_prevents_blocking_consume(pika_env):
    channel = pika_env.FakeChannel()
    connection = pika_env.FakeConnection(channel)
    module = pika_env.import_fresh(
        "src.common.middleware.middleware_rabbitmq", connection=connection
    )

    server = module.MessageMiddlewareRpcServerRabbitMQ("rabbitmq", "rates_requests")
    server.stop()
    server.connect()
    server.start(lambda _body, _reply: None)

    # start() saw the pending stop and never entered the blocking consume.
    assert channel.start_consuming_calls == 0
    assert channel.stop_consuming_calls == 1


def test_rpc_client_call_aborts_on_cancel_event(pika_env):
    channel = pika_env.FakeChannel()
    connection = pika_env.FakeConnection(channel)
    module = pika_env.import_fresh(
        "src.common.middleware.middleware_rabbitmq", connection=connection
    )

    client = module.MessageMiddlewareRpcClientRabbitMQ("rabbitmq", "rates_requests")
    cancel = threading.Event()
    cancel.set()

    # With the cancel event set, the call must abort immediately instead of
    # polling until the (120s) timeout — this is what unblocks the worker's
    # main thread on SIGTERM while loading rates.
    with pytest.raises(module.MessageMiddlewareMessageError):
        client.call(b"get_rates", timeout=120, cancel_event=cancel)
