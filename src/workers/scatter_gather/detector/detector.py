import logging
import os
import struct
from collections import defaultdict

from common import message_protocol
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
SG_DETECTOR_EXCHANGE = os.environ["SG_DETECTOR_EXCHANGE"]
GATEWAY_Q4_QUEUE = os.environ["GATEWAY_Q4_QUEUE"]
MIN_INTERMEDIARIES = int(os.environ.get("MIN_INTERMEDIARIES", "5"))

_ACCOUNT_SIZE = 32
_TRIPLE_FORMAT = f"!{_ACCOUNT_SIZE}s{_ACCOUNT_SIZE}s{_ACCOUNT_SIZE}s"
_TRIPLE_SIZE = struct.calcsize(_TRIPLE_FORMAT)
_PAIR_FORMAT = f"!{_ACCOUNT_SIZE}s{_ACCOUNT_SIZE}s"


def _decode_account(raw: bytes) -> str:
    return raw.rstrip(b"\x00").decode("utf-8")


def _decode_triple(payload: bytes) -> tuple[str, str, str]:
    if len(payload) != _TRIPLE_SIZE:
        raise ValueError(f"invalid scatter-gather triple size: {len(payload)}")

    a, m, b = struct.unpack(_TRIPLE_FORMAT, payload)
    return _decode_account(a), _decode_account(m), _decode_account(b)


def _encode_pair(a: str, b: str) -> bytes:
    return struct.pack(
        _PAIR_FORMAT,
        a.encode("utf-8")[:_ACCOUNT_SIZE].ljust(_ACCOUNT_SIZE, b"\x00"),
        b.encode("utf-8")[:_ACCOUNT_SIZE].ljust(_ACCOUNT_SIZE, b"\x00"),
    )


class ScatterGatherDetector:
    def __init__(self):
        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SG_DETECTOR_EXCHANGE,
            [f"sg_detector_{ID}"],
            queue_name=f"sg_detector_{ID}",
            exclusive=False,
        )
        self._output = MessageMiddlewareQueueRabbitMQ(MOM_HOST, GATEWAY_Q4_QUEUE)
        self._proto = message_protocol.internal.InternalProtocol()

        # client_id -> (A, B) -> {M}
        self._intermediaries = defaultdict(lambda: defaultdict(set))
        self._emitted = defaultdict(set)

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type != message_protocol.common.MessageType.DATA:
                ack()
                return

            a, m, b = _decode_triple(payload)
            self._add_relation(client_id, a, m, b)
            ack()
        except Exception as e:
            logging.error("detector_%s error: %s", ID, e)
            nack()

    def _add_relation(self, client_id: int, a: str, m: str, b: str):
        pair = (a, b)
        if pair in self._emitted[client_id]:
            return

        intermediaries = self._intermediaries[client_id][pair]
        intermediaries.add(m)

        if len(intermediaries) >= MIN_INTERMEDIARIES:
            self._emit(client_id, a, b)
            self._emitted[client_id].add(pair)
            del self._intermediaries[client_id][pair]

    def _emit(self, client_id: int, a: str, b: str):
        msg = self._proto.create_packet(
            msg_type=message_protocol.common.MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=_encode_pair(a, b),
        )
        self._output.send(msg)
        logging.debug("detector_%s emitted (%s, %s)", ID, a, b)

    def start(self):
        logging.info("detector_%s starting", ID)
        self._input.start_consuming(self._on_message)

    def handle_sigterm(self):
        logging.info("detector_%s sigterm", ID)
        self._input.stop_consuming()

    def close(self):
        self._input.close()
        self._output.close()
