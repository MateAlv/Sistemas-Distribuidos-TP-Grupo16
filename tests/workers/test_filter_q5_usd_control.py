from common.message_protocol.internal import InternalProtocol
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)


class CapturingEndpoint:
    instances = []

    def __init__(self, *_args, **_kwargs):
        self.sent = []
        self.closed = False
        CapturingEndpoint.instances.append(self)

    def send(self, message, *args, **kwargs):
        self.sent.append(message)

    def close(self):
        self.closed = True


class CapturingLazyQueue(CapturingEndpoint):
    by_name = {}

    def __init__(self, _host, queue_name):
        super().__init__()
        self.queue_name = queue_name
        CapturingLazyQueue.by_name[queue_name] = self


class Calls:
    def __init__(self):
        self.acks = 0
        self.nacks = []

    def ack(self):
        self.acks += 1

    def nack(self, requeue=False):
        self.nacks.append(requeue)


def _setup_q5_module(pika_env, monkeypatch, tmp_path, worker_id="0", amount="2"):
    monkeypatch.setenv("ID", worker_id)
    monkeypatch.setenv("MOM_HOST", "rabbitmq")
    monkeypatch.setenv("INPUT_EXCHANGE", "filter_q5_usd_exchange")
    monkeypatch.setenv("INPUT_QUEUE", f"filter_q5_usd_{worker_id}")
    monkeypatch.setenv("INPUT_ROUTING_PREFIX", "filter_q5_usd")
    monkeypatch.setenv("AGGREGATION_AMOUNT", "1")
    monkeypatch.setenv("AGGREGATION_PREFIX", "aggregation_q5")
    monkeypatch.setenv("FILTER_Q5_USD_AMOUNT", amount)
    monkeypatch.setenv("STATE_DIR", str(tmp_path))

    CapturingEndpoint.instances = []
    CapturingLazyQueue.by_name = {}

    module = pika_env.import_fresh("workers.filter_q5_usd.filter_q5_usd")
    monkeypatch.setattr(module, "MessageMiddlewareExchangeRabbitMQ", CapturingEndpoint)
    monkeypatch.setattr(module, "MessageMiddlewareQueueRabbitMQ", CapturingEndpoint)
    monkeypatch.setattr(module, "LazyQueue", CapturingLazyQueue)
    return module


def _flush_order_message(module, client_id: int, leader_id: int) -> bytes:
    payload = ControlMessageSerializer.serialize(
        ControlMessage(sender_id=leader_id, expected_total=0, processed_count=0)
    )
    return InternalProtocol.create_packet(
        msg_type=MessageType.FLUSH_ORDER,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=payload,
    )


def test_flush_order_sends_ack_to_dynamic_leader_response_queue(
    pika_env, monkeypatch, tmp_path
):
    module = _setup_q5_module(pika_env, monkeypatch, tmp_path)
    worker = module.FilterQ5UsdWorker()
    calls = Calls()

    client_id = 42
    leader_id = 1
    message = _flush_order_message(module, client_id, leader_id)

    worker._handle_control(message, calls.ack, calls.nack, response_senders={})

    assert calls.acks == 1
    assert calls.nacks == []
    assert worker._coordinator.response_queue_for(leader_id) in CapturingLazyQueue.by_name
    assert len(
        CapturingLazyQueue.by_name[
            worker._coordinator.response_queue_for(leader_id)
        ].sent
    ) == 1
    assert worker._coordinator.response_queue_for(0) not in CapturingLazyQueue.by_name
