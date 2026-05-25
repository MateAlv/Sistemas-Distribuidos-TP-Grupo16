import os
import logging
from collections import defaultdict

from common import message_protocol
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import partition_for_pair
from common.message_protocol.internal.scatter_gather_serializer import (
    ScatterGatherRelation,
    ScatterGatherRelationSerializer,
)
from common.constants import EDGE_A_TO_M, EDGE_M_TO_B

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
SG_LINKER_EXCHANGE = os.environ["SG_LINKER_EXCHANGE"]
SG_DETECTOR_EXCHANGE = os.environ["SG_DETECTOR_EXCHANGE"]
SG_DETECTOR_AMOUNT = int(os.environ["SG_DETECTOR_AMOUNT"])
SG_LINKER_BATCH_BYTES = int(os.environ.get("SG_LINKER_BATCH_BYTES", str(1024 * 1024)))
SG_LINKER_BATCH_MAX_RELATIONS = int(os.environ.get("SG_LINKER_BATCH_MAX_RELATIONS", "5000"))


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
        self._control_serializer = ControlMessageSerializer()

        # per-client state: client_id -> { M -> set(A) / set(B) / set((A,B)) }
        self._incoming = defaultdict(lambda: defaultdict(set))  # [client][M] = {A}
        self._outgoing = defaultdict(lambda: defaultdict(set))  # [client][M] = {B}
        self._emitted  = defaultdict(lambda: defaultdict(set))  # [client][M] = {(A,B)}
        self._emitted_count_by_client = defaultdict(int)
        self._emitted_count_by_partition = defaultdict(lambda: defaultdict(int))
        self._batch_buffers = defaultdict(list)
        self._batch_bytes = defaultdict(int)
        self._eofs_by_client = defaultdict(set)
        self._closed_by_client = set()

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if client_id in self._closed_by_client:
                logging.info(
                    "linker_%s message_for_closed_client | client_id=%s",
                    ID,
                    client_id,
                )
                ack()
                return

            if msg_type == MessageType.EOF:
                self._handle_eof(client_id, payload)
                ack()
                return
            if msg_type != MessageType.DATA:
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
        if a not in self._incoming[client_id][m]:
            self._incoming[client_id][m].add(a)
        for b in self._outgoing[client_id][m]:
            self._try_emit(client_id, a, m, b)

    def _add_outgoing(self, client_id: int, m: str, b: str):
        if b not in self._outgoing[client_id][m]:
            self._outgoing[client_id][m].add(b)
        for a in self._incoming[client_id][m]:
            self._try_emit(client_id, a, m, b)

    def _try_emit(self, client_id: int, a: str, m: str, b: str):
        if (a, b) in self._emitted[client_id][m]:
            return

        partition = partition_for_pair(a, b, SG_DETECTOR_AMOUNT)
        relation = ScatterGatherRelation(
            from_account=a,
            intermediate_account=m,
            to_account=b,
        )
        self._append_relation(client_id, partition, relation)
        self._emitted[client_id][m].add((a, b))
        self._emitted_count_by_client[client_id] += 1
        self._emitted_count_by_partition[client_id][partition] += 1
        logging.debug("linker_%s emitted (%s, %s, %s)", ID, a, m, b)

    def _append_relation(
        self,
        client_id: int,
        partition: int,
        relation: ScatterGatherRelation,
    ) -> None:
        payload = ScatterGatherRelationSerializer.serialize(relation)
        key = (client_id, partition)
        self._batch_buffers[key].append(payload)
        self._batch_bytes[key] += len(payload)
        if (
            len(self._batch_buffers[key]) >= SG_LINKER_BATCH_MAX_RELATIONS
            or self._batch_bytes[key] >= SG_LINKER_BATCH_BYTES
        ):
            self._flush_batch(client_id, partition)

    def _flush_batch(self, client_id: int, partition: int) -> None:
        key = (client_id, partition)
        payloads = self._batch_buffers.pop(key, [])
        self._batch_bytes.pop(key, None)
        if not payloads:
            return

        msg = self._proto.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=b"".join(payloads),
        )
        self._detectors[partition].send(msg)
        logging.debug(
            "linker_%s batch_flush | client_id=%s | detector=%s | relations=%s | bytes=%s",
            ID, client_id, partition, len(payloads), len(msg),
        )

    def _flush_client(self, client_id: int) -> None:
        for partition in range(SG_DETECTOR_AMOUNT):
            self._flush_batch(client_id, partition)

    def _handle_eof(self, client_id: int, payload: bytes):
        control_message = self._control_serializer.deserialize(payload)
        if control_message.sender_id in self._eofs_by_client[client_id]:
            return

        self._eofs_by_client[client_id].add(control_message.sender_id)
        self._flush_client(client_id)

        for partition, detector in enumerate(self._detectors):
            expected_total = self._emitted_count_by_partition[client_id].get(partition, 0)
            control_payload = self._control_serializer.serialize(
                ControlMessage(
                    sender_id=ID,
                    expected_total=expected_total,
                    processed_count=0,
                )
            )
            msg = self._proto.create_packet(
                msg_type=MessageType.EOF,
                client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                payload=control_payload,
            )
            detector.send(msg)

        self._incoming.pop(client_id, None)
        self._outgoing.pop(client_id, None)
        self._emitted.pop(client_id, None)
        self._emitted_count_by_client.pop(client_id, None)
        self._emitted_count_by_partition.pop(client_id, None)
        for key in [key for key in self._batch_buffers if key[0] == client_id]:
            self._batch_buffers.pop(key, None)
            self._batch_bytes.pop(key, None)
        self._eofs_by_client.pop(client_id, None)
        self._closed_by_client.add(client_id)
        logging.info(
            "linker_%s forwarded_eof | client_id=%s | detectors=%s",
            ID,
            client_id,
            len(self._detectors),
        )

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
