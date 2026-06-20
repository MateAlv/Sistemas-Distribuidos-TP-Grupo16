import pytest


def test_publisher_confirms_disabled_by_default(pika_env, monkeypatch):
    monkeypatch.delenv("RABBITMQ_PUBLISHER_CONFIRMS", raising=False)
    channel = pika_env.FakeChannel()
    connection = pika_env.FakeConnection(channel)

    module = pika_env.import_fresh(
        "src.common.middleware.middleware_rabbitmq",
        connection=connection,
    )
    module.MessageMiddlewareQueueRabbitMQ("rabbitmq", "queue")

    assert channel.confirm_delivery_calls == 0


def test_publisher_confirms_enabled_on_channel_init(pika_env, monkeypatch):
    monkeypatch.setenv("RABBITMQ_PUBLISHER_CONFIRMS", "true")
    channel = pika_env.FakeChannel()
    connection = pika_env.FakeConnection(channel)

    module = pika_env.import_fresh(
        "src.common.middleware.middleware_rabbitmq",
        connection=connection,
    )
    module.MessageMiddlewareQueueRabbitMQ("rabbitmq", "queue")

    assert channel.confirm_delivery_calls == 1


def test_queue_send_maps_broker_nack_to_message_error(pika_env, monkeypatch):
    monkeypatch.delenv("RABBITMQ_PUBLISHER_CONFIRMS", raising=False)
    channel = pika_env.FakeChannel()
    connection = pika_env.FakeConnection(channel)

    module = pika_env.import_fresh(
        "src.common.middleware.middleware_rabbitmq",
        connection=connection,
    )
    queue = module.MessageMiddlewareQueueRabbitMQ("rabbitmq", "queue")
    channel.basic_publish_error = module.pika.exceptions.NackError([])

    with pytest.raises(module.MessageMiddlewareMessageError, match="broker nacked"):
        queue.send(b"message")


def test_queue_send_publishes_mandatory(pika_env, monkeypatch):
    monkeypatch.delenv("RABBITMQ_PUBLISHER_CONFIRMS", raising=False)
    channel = pika_env.FakeChannel()
    connection = pika_env.FakeConnection(channel)

    module = pika_env.import_fresh(
        "src.common.middleware.middleware_rabbitmq",
        connection=connection,
    )
    queue = module.MessageMiddlewareQueueRabbitMQ("rabbitmq", "queue")

    queue.send(b"message")

    assert channel.basic_publish_calls[-1][1]["mandatory"] is True


def test_exchange_send_maps_unroutable_to_message_error(pika_env, monkeypatch):
    monkeypatch.delenv("RABBITMQ_PUBLISHER_CONFIRMS", raising=False)
    channel = pika_env.FakeChannel()
    connection = pika_env.FakeConnection(channel)

    module = pika_env.import_fresh(
        "src.common.middleware.middleware_rabbitmq",
        connection=connection,
    )
    exchange = module.MessageMiddlewareExchangeRabbitMQ(
        "rabbitmq",
        "exchange",
        ["routing_key"],
    )
    channel.basic_publish_error = module.pika.exceptions.UnroutableError([])

    with pytest.raises(module.MessageMiddlewareMessageError, match="unroutable"):
        exchange.send(b"message")


def test_exchange_send_publishes_mandatory(pika_env, monkeypatch):
    monkeypatch.delenv("RABBITMQ_PUBLISHER_CONFIRMS", raising=False)
    channel = pika_env.FakeChannel()
    connection = pika_env.FakeConnection(channel)

    module = pika_env.import_fresh(
        "src.common.middleware.middleware_rabbitmq",
        connection=connection,
    )
    exchange = module.MessageMiddlewareExchangeRabbitMQ(
        "rabbitmq",
        "exchange",
        ["routing_key"],
    )

    exchange.send(b"message")

    assert channel.basic_publish_calls[-1][1]["mandatory"] is True
