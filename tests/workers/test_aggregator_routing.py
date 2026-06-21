import importlib

from common.constants import C_Q5


class _DummyQueue:
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

    def start_consuming(self, callback):
        return None

    def request_stop_consuming(self):
        return None

    def close(self):
        return None

    def send(self, message):
        return None


class _DummyExchange:
    created = []

    def __init__(
        self,
        host,
        exchange_name,
        routing_keys,
        exchange_type="direct",
        queue_name=None,
        exclusive=True,
    ):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.exchange_type = exchange_type
        self.queue_name = queue_name
        self.exclusive = exclusive
        self.created.append(self)

    def start_consuming(self, callback):
        return None

    def request_stop_consuming(self):
        return None

    def close(self):
        return None


def test_aggregator_consumes_from_stable_personal_queue(monkeypatch):
    monkeypatch.setenv("ID", "2")
    monkeypatch.setenv("MOM_HOST", "rabbit")
    monkeypatch.setenv("CONFIGURATION", C_Q5)
    monkeypatch.setenv("AGGREGATION_PREFIX", "aggregation_q5")
    monkeypatch.setenv("AGGREGATION_AMOUNT", "3")
    monkeypatch.setenv("OUTPUT_QUEUE", "join_q5_queue")

    module = importlib.import_module("workers.aggregator.aggregators")
    module = importlib.reload(module)
    _DummyExchange.created.clear()
    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", _DummyQueue)
    monkeypatch.setattr(
        module.middleware,
        "MessageMiddlewareExchangeRabbitMQ",
        _DummyExchange,
    )

    worker = module.AggregatorWorker()
    worker.start()

    input_exchange = _DummyExchange.created[-1]
    assert input_exchange.exchange_name == "aggregation_q5"
    assert input_exchange.routing_keys == ["aggregation_q5_2"]
    assert input_exchange.queue_name == "aggregation_q5_2"
    assert input_exchange.exclusive is False
