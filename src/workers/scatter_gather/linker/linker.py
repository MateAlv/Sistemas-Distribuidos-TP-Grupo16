import os
import logging
import struct
from collections import defaultdict

from common import message_protocol
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
)
from common.message_protocol.internal import partition_for_pair
from common.constants import EDGE_A_TO_M, EDGE_M_TO_B

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
SG_LINKER_EXCHANGE = os.environ["SG_LINKER_EXCHANGE"]
SG_DETECTOR_EXCHANGE = os.environ["SG_DETECTOR_EXCHANGE"]
SG_DETECTOR_AMOUNT = int(os.environ["SG_DETECTOR_AMOUNT"])

# (A, M, B) payload: three account strings, each up to 32 bytes
_TRIPLE_FORMAT = "!32s32s32s"
_TRIPLE_SIZE = struct.calcsize(_TRIPLE_FORMAT)


def _encode_triple(a: str, m: str, b: str) -> bytes:
    return struct.pack(
        _TRIPLE_FORMAT,
        a.encode()[:32].ljust(32, b"\x00"),
        m.encode()[:32].ljust(32, b"\x00"),
        b.encode()[:32].ljust(32, b"\x00"),
    )


class ScatterGatherLinker:
    def __init__(self):
        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SG_LINKER_EXCHANGE,
            [f"sg_linker_{ID}"],
            queue_name=f"sg_linker_{ID}",
            exclusive=False,
        )
        self._detectors = [
            MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SG_DETECTOR_EXCHANGE, [f"sg_detector_{i}"]
            )
            for i in range(SG_DETECTOR_AMOUNT)
        ]
        self._tx_ser = message_protocol.internal.TransactionSerializer()
        self._proto = message_protocol.internal.InternalProtocol()

        # per-client state: client_id -> { M -> set(A) / set(B) / set((A,B)) }
        self._incoming = defaultdict(lambda: defaultdict(set))  # [client][M] = {A}
        self._outgoing = defaultdict(lambda: defaultdict(set))  # [client][M] = {B}
        self._emitted  = defaultdict(lambda: defaultdict(set))  # [client][M] = {(A,B)}

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type != message_protocol.common.MessageType.DATA:
                ack()
                return

            edge_tag = payload[0]
            tx = self._tx_ser.deserialize(payload[1:])

            if edge_tag == EDGE_A_TO_M:
                # to_account is M, from_account is A
                self._add_incoming(client_id, m=tx.to_account, a=tx.from_account)
            elif edge_tag == EDGE_M_TO_B:
                # from_account is M, to_account is B
                self._add_outgoing(client_id, m=tx.from_account, b=tx.to_account)
            else:
                logging.warning("linker_%s unknown edge tag %s", ID, edge_tag)

            ack()
        except Exception as e:
            logging.error("linker_%s error: %s", ID, e)
            nack()

    def _add_incoming(self, client_id: int, m: str, a: str):
        if a in self._incoming[client_id][m]:
            return
        self._incoming[client_id][m].add(a)
        for b in self._outgoing[client_id][m]:
            self._try_emit(client_id, a, m, b)

    def _add_outgoing(self, client_id: int, m: str, b: str):
        if b in self._outgoing[client_id][m]:
            return
        self._outgoing[client_id][m].add(b)
        for a in self._incoming[client_id][m]:
            self._try_emit(client_id, a, m, b)

    def _try_emit(self, client_id: int, a: str, m: str, b: str):
        if (a, b) in self._emitted[client_id][m]:
            return
        self._emitted[client_id][m].add((a, b))

        partition = partition_for_pair(a, b, SG_DETECTOR_AMOUNT)
        msg = self._proto.create_packet(
            msg_type=message_protocol.common.MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=_encode_triple(a, m, b),
        )
        self._detectors[partition].send(msg)
        logging.debug("linker_%s emitted (%s, %s, %s)", ID, a, m, b)

    def start(self):
        logging.info("linker_%s starting", ID)
        self._input.start_consuming(self._on_message)

    def handle_sigterm(self):
        logging.info("linker_%s sigterm", ID)
        self._input.stop_consuming()

    def close(self):
        self._input.close()
        for detector in self._detectors:
            detector.close()
