import logging
import os
from collections import defaultdict

from common import message_protocol
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.message_protocol.common import ControlMessage, MessageType
from common.message_protocol.control_message_serializer import ControlMessageSerializer
from common.message_protocol.scatter_gather_serializer import (
    ScatterGatherResult,
    ScatterGatherResultSerializer,
    ScatterGatherRelationSerializer,
)

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
SG_DETECTOR_EXCHANGE = os.environ["SG_DETECTOR_EXCHANGE"]
GATEWAY_Q4_QUEUE = os.environ["GATEWAY_Q4_QUEUE"]
MIN_INTERMEDIARIES = int(os.environ.get("MIN_INTERMEDIARIES", "5"))
SG_LINKER_AMOUNT = int(os.environ.get("SG_LINKER_AMOUNT", "1"))


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
        self._control_serializer = ControlMessageSerializer()

        # client_id -> (A, B) -> {M}
        self._intermediaries = defaultdict(lambda: defaultdict(set))
        self._emitted = defaultdict(set)
        self._eofs_by_client = defaultdict(int)

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.EOF:
                self._handle_eof(client_id)
                ack()
                return
            if msg_type != MessageType.DATA:
                ack()
                return

            relation = ScatterGatherRelationSerializer.deserialize(payload)
            self._add_relation(
                client_id,
                relation.from_account,
                relation.intermediate_account,
                relation.to_account,
            )
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
            msg_type=MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=ScatterGatherResultSerializer.serialize(
                ScatterGatherResult(from_account=a, to_account=b)
            ),
        )
        self._output.send(msg)
        logging.debug("detector_%s emitted (%s, %s)", ID, a, b)

    def _handle_eof(self, client_id: int):
        self._eofs_by_client[client_id] += 1
        if self._eofs_by_client[client_id] < SG_LINKER_AMOUNT:
            return

        payload = self._control_serializer.serialize(
            ControlMessage(
                sender_id=ID,
                expected_total=len(self._emitted.get(client_id, ())),
                processed_count=0,
            )
        )
        msg = self._proto.create_packet(
            msg_type=MessageType.EOF,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
        self._output.send(msg)
        self._intermediaries.pop(client_id, None)
        self._emitted.pop(client_id, None)
        self._eofs_by_client.pop(client_id, None)
        logging.info("detector_%s forwarded_eof | client_id=%s", ID, client_id)

    def start(self):
        logging.info("detector_%s starting", ID)
        self._input.start_consuming(self._on_message)

    def handle_sigterm(self):
        logging.info("detector_%s sigterm", ID)
        self._input.stop_consuming()

    def close(self):
        self._input.close()
        self._output.close()
