"""
Publisher que rutea mensajes a N shards según el client_id extraído del header
del InternalProtocol.
"""
from .middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
)
from common.routing import routing_key_for_shard, shard_for_client_id


_CLIENT_ID_OFFSET = 1   # 1 byte de msg_type antes del client_id
_CLIENT_ID_SIZE = 16    # InternalProtocol.HEADER_FORMAT = "!B 16s"


class ShardedByClientPublisher:
    def __init__(
        self,
        mom_host: str,
        exchange_name: str,
        routing_key_prefix: str,
        shard_count: int,
    ) -> None:
        if shard_count < 1:
            raise ValueError("shard_count must be >= 1")
        self._shard_count = shard_count
        self._publishers = [
            MessageMiddlewareExchangeRabbitMQ(
                mom_host,
                exchange_name,
                [routing_key_for_shard(routing_key_prefix, i)],
            )
            for i in range(shard_count)
        ]

    def send(self, message: bytes) -> None:
        client_id = int.from_bytes(
            message[_CLIENT_ID_OFFSET:_CLIENT_ID_OFFSET + _CLIENT_ID_SIZE],
            "big",
        )
        shard = shard_for_client_id(client_id, self._shard_count)
        self._publishers[shard].send(message)

    def close(self) -> None:
        for publisher in self._publishers:
            try:
                publisher.close()
            except Exception:
                pass
