"""Data-plane unit tests for the WAL-wired filter, one codebase per CONFIGURATION.

Covers: per-config routing (which outputs a transaction is forwarded to), DATA
dedup via the durable handler (the Action.ACK path), and the N=1 upstream-EOF
flush (drain the durable batcher + downstream EOF + close). The full broadcast
EOF protocol (N>1, FLUSH_ACK) is covered end-to-end by the integration tests.
"""

import importlib
import json
import sys
import types

from common.constants import C_Q1, C_Q5, C_USD
from common.domain.transaction import Transaction
from common.message_protocol.internal import InternalProtocol, TransactionSerializer
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer


class RecordingSender:
    def __init__(self, *args, **kwargs):
        self.messages = []

    def send(self, message, *args, **kwargs):
        self.messages.append(message)

    def close(self):
        pass

    def start_consuming(self, *_):
        pass

    def stop_consuming(self):
        pass


class RecordingPublishers(dict):
    """Publisher map that auto-creates a RecordingSender for any destination, so a
    test never needs to enumerate the worker's routing keys up front. The worker's
    _publish uses .get(); the tests use [] — both auto-create."""

    def get(self, key, default=None):
        return self[key]

    def __missing__(self, key):
        sender = RecordingSender()
        self[key] = sender
        return sender


def _noop_ack():
    pass


def _noop_nack(requeue=False):
    pass


def _tx(amount=10.0, currency="US Dollar", fmt="Wire") -> Transaction:
    return Transaction(
        date="2022/09/01 00:08",
        from_bank="bank_1",
        from_account="acc_a",
        to_bank="bank_2",
        to_account="acc_b",
        amount=amount,
        currency=currency,
        format=fmt,
    )


def _addressed(msg_type, client_id, sender_id, seq, payload) -> bytes:
    return InternalProtocol.create_addressed_packet(
        msg_type=msg_type,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        sender_id=sender_id,
        seq=seq,
        payload=payload,
    )


def _import_filter(monkeypatch, tmp_path, configuration, batch_max_tx="1000", filter_amount="1"):
    env = {
        "ID": "0",
        "MOM_HOST": "rabbitmq",
        "CONFIGURATION": configuration,
        "INPUT_QUEUE": f"filter_{configuration}_0",
        "GATEWAY_QUEUE": "gateway_queue",
        "FILTER_DATE_QUEUE": "filter_date",
        "FILTER_Q1_QUEUE": "filter_q1",
        "FILTER_Q3_QUEUE": "filter_q3",
        "SCATTER_GATHER_MAPPER_QUEUE": "sg_mapper",
        "FILTER_Q5_USD_QUEUE": "filter_q5_usd",
        "SUM_PREFIX": "sum",
        "FILTER_AMOUNT": filter_amount,
        "FILTER_PREFIX": "filter",
        "STATE_DIR": str(tmp_path),
        "FILTER_OUTPUT_BATCH_MAX_TX": batch_max_tx,
    }
    if configuration == C_USD:
        env.update({
            "FILTER_Q1_EXCHANGE": "filter_q1_exchange",
            "FILTER_Q1_ROUTING_PREFIX": "filter_q1", "FILTER_Q1_AMOUNT": "1",
            "SUM_Q2_EXCHANGE": "sum_q2_exchange",
            "SUM_Q2_ROUTING_PREFIX": "sum_q2", "SUM_Q2_AMOUNT": "1",
            "FILTER_DATE_EXCHANGE": "filter_date_exchange",
            "FILTER_DATE_ROUTING_PREFIX": "filter_date", "FILTER_DATE_AMOUNT": "1",
        })
    if configuration == C_Q5:
        env.update({
            "FILTER_Q5_USD_EXCHANGE": "filter_q5_usd_exchange",
            "FILTER_Q5_USD_ROUTING_PREFIX": "filter_q5_usd", "FILTER_Q5_USD_AMOUNT": "1",
        })
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    fake_pika = types.SimpleNamespace(
        exceptions=types.SimpleNamespace(
            AMQPConnectionError=Exception,
            AMQPChannelError=Exception,
            StreamLostError=Exception,
        ),
        BasicProperties=lambda *a, **k: None,
        BlockingConnection=lambda *a, **k: None,
        ConnectionParameters=lambda *a, **k: None,
    )
    monkeypatch.setitem(sys.modules, "pika", fake_pika)
    sys.modules.pop("workers.filter.filters", None)
    module = importlib.import_module("workers.filter.filters")
    monkeypatch.setattr(module.middleware, "MessageMiddlewareQueueRabbitMQ", RecordingSender)
    monkeypatch.setattr(module.middleware, "MessageMiddlewareExchangeRabbitMQ", RecordingSender)
    monkeypatch.setattr(module.middleware, "LazyQueue", RecordingSender)
    monkeypatch.setattr(module.middleware, "ShardedPublisher", RecordingSender)
    monkeypatch.setattr(module.middleware, "ShardedByClientPublisher", RecordingSender)
    return module


def _worker(module):
    """Construct a FilterWorker and wire the data-thread runner with recording
    publishers (auto-creating per destination)."""
    from common.fault_tolerance.handler import WorkerRunner

    worker = module.FilterWorker()
    publishers = RecordingPublishers()
    worker._data_publishers = publishers
    worker._runner = WorkerRunner(
        handler=worker._handler,
        publishers=publishers,
        process_payload=worker._data_process_payload,
        lock=worker.lock,
    )
    worker._runner.recover_and_republish()
    return worker, publishers


def _data_change_for(worker, client_id, transactions):
    payload = TransactionSerializer.serialize_batch(transactions)
    change, _outputs = worker._data_process_payload(client_id, payload)
    return change


# ─── routing per CONFIGURATION ──────────────────────────────────────────────

def test_usd_routes_us_dollar_to_enabled_outputs(monkeypatch, tmp_path):
    module = _import_filter(monkeypatch, tmp_path, C_USD)
    worker, _ = _worker(module)

    # 2 US Dollar (pass) + 1 Euro (filtered out).
    change = _data_change_for(worker, 1, [_tx(currency="US Dollar"),
                                          _tx(currency="US Dollar"),
                                          _tx(currency="Euro")])
    fwd = change["forwarded_by_output"]
    assert fwd == {
        module.FILTER_Q1_QUEUE: 2,
        module.SUM_Q2_OUTPUT: 2,
        module.FILTER_DATE_QUEUE: 2,
    }
    worker._state.apply_change(change)
    assert worker._state.processed_count(1) == 3
    assert worker._state.forwarded_by_output(1) == fwd


def test_q1_routes_only_small_amounts_to_gateway(monkeypatch, tmp_path):
    module = _import_filter(monkeypatch, tmp_path, C_Q1)
    worker, _ = _worker(module)

    change = _data_change_for(worker, 1, [_tx(amount=10.0), _tx(amount=100.0), _tx(amount=49.0)])
    assert change["forwarded_by_output"] == {module.GATEWAY_QUEUE: 2}  # 10 and 49 pass; 100 filtered


def test_q5_routes_wire_and_ach_to_filter_q5_usd(monkeypatch, tmp_path):
    module = _import_filter(monkeypatch, tmp_path, C_Q5)
    worker, _ = _worker(module)

    change = _data_change_for(worker, 1, [_tx(fmt="Wire"), _tx(fmt="ACH"), _tx(fmt="Cash")])
    assert change["forwarded_by_output"] == {module.FILTER_Q5_USD_QUEUE: 2}


# ─── DATA dedup (Action.ACK path) ───────────────────────────────────────────

def test_duplicate_data_is_ignored(monkeypatch, tmp_path):
    module = _import_filter(monkeypatch, tmp_path, C_Q1)
    worker, _ = _worker(module)
    packet = _addressed(
        MessageType.DATA, 1, sender_id=7, seq=0,
        payload=TransactionSerializer.serialize_batch([_tx(amount=10.0), _tx(amount=20.0)]),
    )

    worker._runner.process(packet, _noop_ack, _noop_nack)
    worker._runner.process(packet, _noop_ack, _noop_nack)  # redelivery, same (sender, seq)

    assert worker._state.processed_count(1) == 2  # counted once, not four times


# ─── N=1 upstream EOF: drain batcher + downstream EOF + close ────────────────

def test_n1_eof_flushes_buffer_and_closes(monkeypatch, tmp_path):
    module = _import_filter(monkeypatch, tmp_path, C_Q1)
    worker, publishers = _worker(module)
    control_serializer = ControlMessageSerializer()

    # Feed a DATA batch; with the default large batch limit it stays buffered.
    data = _addressed(
        MessageType.DATA, 1, sender_id=7, seq=0,
        payload=TransactionSerializer.serialize_batch([_tx(amount=10.0), _tx(amount=20.0)]),
    )
    worker._runner.process(data, _noop_ack, _noop_nack)
    assert publishers[module.GATEWAY_QUEUE].messages == []  # buffered, not flushed yet

    # Upstream EOF (N=1): drain the buffer + emit downstream EOF, then close.
    eof = _addressed(
        MessageType.EOF, 1, sender_id=7, seq=1,
        payload=control_serializer.serialize(
            ControlMessage(sender_id=7, expected_total=2, processed_count=0)
        ),
    )
    worker._handle_upstream_eof(eof, _noop_ack, _noop_nack)

    sent = publishers[module.GATEWAY_QUEUE].messages
    assert len(sent) == 2  # one drained DATA batch + one EOF
    # GATEWAY is a basic (non-addressed) edge.
    data_msg_type, _, _ = InternalProtocol.unpack_packet(sent[0])
    eof_msg_type, _, eof_payload = InternalProtocol.unpack_packet(sent[1])
    assert data_msg_type == MessageType.DATA
    assert eof_msg_type == MessageType.EOF
    assert control_serializer.deserialize(eof_payload).expected_total == 2
    assert worker._state.is_closed(1)


# ─── N>1 leader: last custom FLUSH_ACK emits the consolidated downstream EOF ──

def test_leader_flush_ack_emits_total_and_closes(monkeypatch, tmp_path):
    module = _import_filter(monkeypatch, tmp_path, C_Q1, filter_amount="2")
    worker, publishers = _worker(module)
    control_serializer = ControlMessageSerializer()
    client_id = 1

    # This replica is the leader for the client (it set _leader_expected).
    worker.coordinator._leader_expected[client_id] = 5
    # Leader's own forwarded: 3 transactions buffered to the gateway.
    own = _data_change_for(worker, client_id, [_tx(amount=10.0)] * 3)
    worker._state.apply_change(own)
    assert worker._state.forwarded_by_output(client_id) == {module.GATEWAY_QUEUE: 3}

    # Last (and only, N=2 -> N-1=1) FLUSH_ACK from the non-leader, custom JSON
    # payload with its per-output forwarded counts.
    ack_payload = json.dumps(
        {"sender_id": 1, "forwarded_by_output": {module.GATEWAY_QUEUE: 2}}
    ).encode("utf-8")
    flush_ack = InternalProtocol.create_packet(
        MessageType.FLUSH_ACK, client_id.to_bytes(16, byteorder="big"), ack_payload
    )
    worker._handle_flush_ack(flush_ack, client_id, ack_payload, _noop_ack, _noop_nack, publishers)

    sent = publishers[module.GATEWAY_QUEUE].messages
    assert len(sent) == 2  # leader's drained DATA batch + the consolidated EOF
    data_msg_type, _, _ = InternalProtocol.unpack_packet(sent[0])
    eof_msg_type, _, eof_payload = InternalProtocol.unpack_packet(sent[1])
    assert data_msg_type == MessageType.DATA
    assert eof_msg_type == MessageType.EOF
    # total = own (3) + this ack (2) + prior acks (0).
    assert control_serializer.deserialize(eof_payload).expected_total == 5
    assert worker._state.is_closed(client_id)
