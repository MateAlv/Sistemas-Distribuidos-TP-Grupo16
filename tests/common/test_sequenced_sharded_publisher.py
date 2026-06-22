"""SequencedShardedPublisher stamps addressed packets and keeps a dense seq per
(client, destination shard) so the downstream dedup tracker stays bounded."""

import common.middleware.sharded_publisher as spm
from common.message_protocol.internal import InternalProtocol, MessageType
from common.routing import routing_key_for_shard, shard_for_key


class FakeExchange:
    def __init__(self, *args, **kwargs):
        self.sent = []  # (message, routing_key)

    def send(self, message, routing_key=None):
        self.sent.append((message, routing_key))

    def close(self):
        pass


def _publisher(monkeypatch, shard_count=4, sender_id=5):
    monkeypatch.setattr(spm, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    return spm.SequencedShardedPublisher("host", "exch", "pref", shard_count, sender_id)


def test_stamps_addressed_packet_and_routes_by_payload_digest(monkeypatch):
    pub = _publisher(monkeypatch, shard_count=4, sender_id=5)

    pub.send(MessageType.DATA, 7, b"hello")

    message, routing_key = pub._publisher.sent[0]
    msg_type, client_id, sender_id, seq, payload = (
        InternalProtocol.unpack_addressed_packet(message)
    )
    assert msg_type == MessageType.DATA
    assert client_id == 7
    assert sender_id == 5
    assert seq == 0
    assert payload == b"hello"
    expected_shard = shard_for_key(b"hello", 4)
    assert routing_key == routing_key_for_shard("pref", expected_shard)


def test_seq_is_dense_per_client_and_shard(monkeypatch):
    pub = _publisher(monkeypatch, shard_count=4)
    a, b, shard_a, shard_b = _two_payloads_on_different_shards(4)

    pub.send(MessageType.DATA, 7, a)   # client 7, shard_a -> seq 0
    pub.send(MessageType.DATA, 7, b)   # client 7, shard_b -> seq 0 (separate channel)
    pub.send(MessageType.DATA, 7, a)   # client 7, shard_a -> seq 1
    pub.send(MessageType.DATA, 8, a)   # client 8, shard_a -> seq 0 (per-client)

    seqs = [InternalProtocol.unpack_addressed_packet(m)[3] for m, _ in pub._publisher.sent]
    assert seqs == [0, 0, 1, 0]
    assert shard_a != shard_b


def _two_payloads_on_different_shards(shard_count):
    base = b"x0"
    base_shard = shard_for_key(base, shard_count)
    for i in range(1, 1000):
        candidate = f"x{i}".encode()
        if shard_for_key(candidate, shard_count) != base_shard:
            return base, candidate, base_shard, shard_for_key(candidate, shard_count)
    raise AssertionError("could not find two payloads on different shards")
