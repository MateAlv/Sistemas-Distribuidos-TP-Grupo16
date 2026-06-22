"""
Publisher que rutea mensajes a N shards eligiendo la routing key por mensaje.

Un exchange direct rutea por routing key, así que un solo publisher (una sola
conexión AMQP) alcanza para llegar a cualquier shard: elegimos la routing key por
mensaje. Antes abríamos una conexión por shard, lo que multiplicaba las conexiones
a RabbitMQ por cada edge sharded.

La shard se deriva de una clave determinística extraída del mensaje (`key_fn`).
Por defecto se usa el client_id del header, pero las etapas stateless/combiner
rutean por digest del cuerpo para repartir la carga entre todos los workers sin
importar cuántos clientes haya (mismo cuerpo → misma shard → reentrega estable).
"""
from typing import Callable, Union

from .middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
)
from common.message_protocol.internal.protocol import InternalProtocol
from common.routing import routing_key_for_shard, shard_for_key


_CLIENT_ID_OFFSET = 1   # 1 byte de msg_type antes del client_id
_CLIENT_ID_SIZE = 16    # InternalProtocol.HEADER_FORMAT = "!B 16s"
_HEADER_SIZE = _CLIENT_ID_OFFSET + _CLIENT_ID_SIZE

ShardKey = Union[int, str, bytes]
KeyFn = Callable[[bytes], ShardKey]


def client_id_key(message: bytes) -> int:
    """Shard key = client_id del header. Co-loca todo un cliente en una shard."""
    return int.from_bytes(
        message[_CLIENT_ID_OFFSET:_CLIENT_ID_OFFSET + _CLIENT_ID_SIZE],
        "big",
    )


def body_digest_key(message: bytes) -> bytes:
    """Shard key = cuerpo del mensaje (payload tras el header).

    Determinística y de alta cardinalidad: reparte batches entre todas las shards
    aunque haya pocos clientes. El mismo cuerpo siempre cae en la misma shard, así
    que una reentrega tras una caída vuelve al mismo worker (necesario para dedup).
    """
    return message[_HEADER_SIZE:]


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

    def send_to_all(self, message: bytes) -> None:
        """Enviar el mismo mensaje a TODAS las shards (fan-out por routing key).

        Útil para EOF en modo flush_order, donde cada shard recibe su propio EOF.
        """
        for shard in range(self._shard_count):
            routing_key = routing_key_for_shard(self._routing_key_prefix, shard)
            self._publisher.send(message, routing_key=routing_key)

    def close(self) -> None:
        try:
            self._publisher.close()
        except Exception:
            pass


class SequencedShardedPublisher:
    """Publica paquetes *direccionados* (con sender_id y seq) sobre un edge sharded.

    Cada salida lleva un ``seq`` por ``(client_id, shard de destino)``: como el
    sharding por digest reparte un cliente entre todas las shards, un ``seq`` por
    cliente dejaría a cada shard viendo ~1/N de los seqs (huecos sin fin en el
    DeduplicationTracker del consumidor). Con seq por (client, shard) cada shard
    recibe una secuencia densa y el dedup queda acotado.

    La shard se deriva del *payload* (mismo digest que ``body_digest_key``), así
    que coincide con la asignación del publisher plano. El seq es en memoria: con
    el handler durable este rol pasa al ``SenderSequencer`` (que deberá adoptar la
    misma clave (client, shard) antes de dedup-ar un edge sharded aguas abajo).
    """

    def __init__(
        self,
        mom_host: str,
        exchange_name: str,
        routing_key_prefix: str,
        shard_count: int,
        sender_id: int,
    ) -> None:
        if shard_count < 1:
            raise ValueError("shard_count must be >= 1")
        self._shard_count = shard_count
        self._routing_key_prefix = routing_key_prefix
        self._sender_id = sender_id
        self._next_seq: dict[tuple[int, int], int] = {}
        self._publisher = MessageMiddlewareExchangeRabbitMQ(
            mom_host,
            exchange_name,
            [],
        )

    def send(self, msg_type: int, client_id: int, payload: bytes) -> None:
        shard = shard_for_key(payload, self._shard_count)
        seq_key = (client_id, shard)
        seq = self._next_seq.get(seq_key, 0)
        self._next_seq[seq_key] = seq + 1
        message = InternalProtocol.create_addressed_packet(
            msg_type,
            client_id.to_bytes(16, byteorder="big"),
            self._sender_id,
            seq,
            payload,
        )
        routing_key = routing_key_for_shard(self._routing_key_prefix, shard)
        self._publisher.send(message, routing_key=routing_key)

    def close(self) -> None:
        try:
            self._publisher.close()
        except Exception:
            pass


class ShardedByClientPublisher(ShardedPublisher):
    """Variante que rutea por client_id (compatibilidad con llamadores existentes)."""

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
