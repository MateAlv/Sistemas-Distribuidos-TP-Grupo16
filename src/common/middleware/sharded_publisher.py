"""
Publisher que rutea mensajes a N shards según el client_id extraído del header
del InternalProtocol.

Duck-typeado con MessageMiddlewareQueueRabbitMQ: expone `.send(message)` y
`.close()`. Internamente mantiene N MessageMiddlewareExchangeRabbitMQ (uno por
routing key) y elige el shard parseando los 16 bytes de client_id del header
(offset 1, longitud 16, big-endian — ver InternalProtocol.HEADER_FORMAT).

Uso típico:
    publisher = ShardedByClientPublisher(
        mom_host="rabbitmq",
        exchange_name="q3_candidates_exchange",
        routing_key_prefix="q3_candidates",
        shard_count=3,
    )
    publisher.send(packet)   # packet = InternalProtocol.create_packet(...)
    # → publica a routing_key "q3_candidates_{client_id % 3}"
"""
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
)


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
                [f"{routing_key_prefix}_{i}"],
            )
            for i in range(shard_count)
        ]

    def send(self, message: bytes) -> None:
        client_id = int.from_bytes(
            message[_CLIENT_ID_OFFSET:_CLIENT_ID_OFFSET + _CLIENT_ID_SIZE],
            "big",
        )
        shard = client_id % self._shard_count
        self._publishers[shard].send(message)

    def close(self) -> None:
        for publisher in self._publishers:
            try:
                publisher.close()
            except Exception:
                pass
