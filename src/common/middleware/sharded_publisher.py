"""Publisher that routes messages to N shards, choosing the routing key per message.

A direct exchange routes by routing key, so one publisher (one AMQP connection) can
reach any shard by picking the routing key per message. The shard comes from a
deterministic key extracted from the message (key_fn): by default the header's
client_id, but stateless and combiner stages route by body digest so load spreads
across all workers regardless of client count (same body, same shard, so redelivery
is stable).
"""
from typing import Callable, Union

from .middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
)
from common.message_protocol.internal.protocol import InternalProtocol
from common.routing import routing_key_for_shard, shard_for_key


_CLIENT_ID_OFFSET = 1   # 1 byte of msg_type before the client_id
_CLIENT_ID_SIZE = 16    # InternalProtocol.HEADER_FORMAT = "!B 16s"
_HEADER_SIZE = _CLIENT_ID_OFFSET + _CLIENT_ID_SIZE

ShardKey = Union[int, str, bytes]
KeyFn = Callable[[bytes], ShardKey]


def client_id_key(message: bytes) -> int:
    """Shard key is the header's client_id, co-locating a client on one shard."""
    return int.from_bytes(
        message[_CLIENT_ID_OFFSET:_CLIENT_ID_OFFSET + _CLIENT_ID_SIZE],
        "big",
    )


def body_digest_key(message: bytes) -> bytes:
    """Shard key is the message body (payload after the header).

    High-cardinality, so it spreads batches across all shards even with few
    clients. The same body always lands on the same shard, so a redelivery after a
    crash returns to the same worker (needed for dedup).
    """
    return message[_HEADER_SIZE:]


def addressed_body_digest_key(message: bytes) -> bytes:
    """Like body_digest_key but for addressed packets.

    The payload starts after the wider addressed header (it adds sender_id and
    seq). Returns the same bytes the SenderSequencer hashes when assigning the seq
    per (client, edge, shard), so the shard chosen here matches the one the seq was
    reserved for.
    """
    return message[InternalProtocol.ADDRESSED_HEADER_SIZE:]


class ShardedPublisher:
    def __init__(
        self,
        mom_host: str,
        exchange_name: str,
        routing_key_prefix: str,
        shard_count: int,
        key_fn: KeyFn = client_id_key,
    ) -> None:
        if shard_count < 1:
            raise ValueError("shard_count must be >= 1")
        self._shard_count = shard_count
        self._routing_key_prefix = routing_key_prefix
        self._key_fn = key_fn
        self._publisher = MessageMiddlewareExchangeRabbitMQ(
            mom_host,
            exchange_name,
            [],
        )

    def _routing_key_for(self, message: bytes) -> str:
        shard = shard_for_key(self._key_fn(message), self._shard_count)
        return routing_key_for_shard(self._routing_key_prefix, shard)

    def send(self, message: bytes) -> None:
        self._publisher.send(message, routing_key=self._routing_key_for(message))

    def send_to_shard(self, message: bytes, shard: int) -> None:
        """Route to an explicit shard, bypassing key_fn. Used by workers that
        pick the destination partition themselves (semantic partitioning)."""
        if not 0 <= shard < self._shard_count:
            raise ValueError(f"shard {shard} out of range for {self._shard_count} shards")
        routing_key = routing_key_for_shard(self._routing_key_prefix, shard)
        self._publisher.send(message, routing_key=routing_key)

    def send_to_all(self, message: bytes) -> None:
        """Send the same message to every shard (fan-out by routing key), used for
        EOF in flush_order mode where each shard receives its own EOF."""
        for shard in range(self._shard_count):
            routing_key = routing_key_for_shard(self._routing_key_prefix, shard)
            self._publisher.send(message, routing_key=routing_key)

    def close(self) -> None:
        try:
            self._publisher.close()
        except Exception:
            pass


class ShardedByClientPublisher(ShardedPublisher):
    """Routes by client_id."""

    def __init__(
        self,
        mom_host: str,
        exchange_name: str,
        routing_key_prefix: str,
        shard_count: int,
    ) -> None:
        super().__init__(
            mom_host,
            exchange_name,
            routing_key_prefix,
            shard_count,
            key_fn=client_id_key,
        )
