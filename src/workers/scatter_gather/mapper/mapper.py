import os
import logging

from common import message_protocol
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
)
from common.message_protocol.internal import partition_for_key
from common.message_protocol.common import MessageType
from common.constants import EDGE_A_TO_M, EDGE_M_TO_B

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SG_LINKER_EXCHANGE = os.environ["SG_LINKER_EXCHANGE"]
SG_LINKER_AMOUNT = int(os.environ["SG_LINKER_AMOUNT"])


class ScatterGatherMapper:
    def __init__(self):
        self._input = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self._linkers = [
            MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SG_LINKER_EXCHANGE, [f"sg_linker_{i}"]
            )
            for i in range(SG_LINKER_AMOUNT)
        ]
        self._tx_ser = message_protocol.internal.TransactionSerializer()
        self._proto = message_protocol.internal.InternalProtocol()

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.EOF:
                self._forward_eof(client_id, payload)
                ack()
                return
            if msg_type != MessageType.DATA:
                ack()
                return

            tx = self._tx_ser.deserialize(payload)
            cid = client_id.to_bytes(16, byteorder='big')

            self._emit(cid, EDGE_A_TO_M, payload, partition_for_key(tx.to_account, SG_LINKER_AMOUNT))
            self._emit(cid, EDGE_M_TO_B, payload, partition_for_key(tx.from_account, SG_LINKER_AMOUNT))

            ack()
        except Exception as e:
            logging.error("mapper_%s error: %s", ID, e)
            nack()

    def _emit(self, cid: bytes, tag: int, tx_bytes: bytes, partition: int):
        msg = self._proto.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=cid,
            payload=bytes([tag]) + tx_bytes,
        )
        self._linkers[partition].send(msg)

    def _forward_eof(self, client_id: int, payload: bytes):
        msg = self._proto.create_packet(
            msg_type=MessageType.EOF,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
        for linker in self._linkers:
            linker.send(msg)
        logging.info(
            "mapper_%s forwarded_eof | client_id=%s | linkers=%s",
            ID,
            client_id,
            len(self._linkers),
        )

    def start(self):
        logging.info("mapper_%s starting", ID)
        self._input.start_consuming(self._on_message)

    def handle_sigterm(self):
        logging.info("mapper_%s sigterm", ID)
        self._input.stop_consuming()

    def close(self):
        self._input.close()
        for linker in self._linkers:
            linker.close()
