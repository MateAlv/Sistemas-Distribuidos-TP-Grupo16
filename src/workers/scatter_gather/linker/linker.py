import os
import logging
from collections import defaultdict

from common import message_protocol
from common.batch_buffer import BatchBuffer
from common.logging_utils import should_log_progress
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

        # per-client join state: client_id -> { M -> set of neighbour accounts }.
        # These two sets are also our dedup: a repeated edge re-adds an account
        # that is already present, so it is a no-op and emits nothing again.
        self._incoming = defaultdict(lambda: defaultdict(set))  # [client][M] = {A}
        self._outgoing = defaultdict(lambda: defaultdict(set))  # [client][M] = {B}
        self._emitted_count_by_client = defaultdict(int)
        self._emitted_count_by_partition = defaultdict(lambda: defaultdict(int))
        self._data_batches_by_client = defaultdict(int)
        self._edges_received_by_client = defaultdict(int)
        # Coalesces emitted relations into batches keyed by (client_id, detector
        # partition) before they reach a detector.
        self._batcher = BatchBuffer(
            SG_LINKER_BATCH_BYTES, SG_LINKER_BATCH_MAX_RELATIONS
        )
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

            # The mapper batches many same-tagged edges into one message:
            # a single tag byte followed by a batch of serialized transactions.
            edge_tag = payload[0]
            transactions = self._tx_ser.deserialize_batch(payload[1:])

            if edge_tag == EDGE_A_TO_M:
                # to_account is M, from_account is A
                for tx in transactions:
                    self._add_incoming(client_id, m=tx.to_account, a=tx.from_account)
            elif edge_tag == EDGE_M_TO_B:
                # from_account is M, to_account is B
                for tx in transactions:
                    self._add_outgoing(client_id, m=tx.from_account, b=tx.to_account)
            else:
                logging.warning("linker_%s unknown edge tag %s", ID, edge_tag)

            self._data_batches_by_client[client_id] += 1
            self._edges_received_by_client[client_id] += len(transactions)
            if should_log_progress(self._data_batches_by_client[client_id]):
                logging.info(
                    "linker_%s data_batch | client_id=%s | edge_tag=%s | "
                    "batch_size=%s | batches=%s | edges_received=%s | "
                    "relations_emitted=%s",
                    ID,
                    client_id,
                    edge_tag,
                    len(transactions),
                    self._data_batches_by_client[client_id],
                    self._edges_received_by_client[client_id],
                    self._emitted_count_by_client[client_id],
                )

            ack()
        except Exception as e:
            logging.error("linker_%s error: %s", ID, e)
            nack()

    def _add_incoming(self, client_id: int, m: str, a: str):
        # Only a *new* A produces relations: pair it with every B already known
        # for this M. A repeated A is skipped, so each (A, M, B) is emitted once.
        if a not in self._incoming[client_id][m]:
            self._incoming[client_id][m].add(a)
            for b in self._outgoing[client_id][m]:
                self._emit_relation(client_id, a, m, b)

    def _add_outgoing(self, client_id: int, m: str, b: str):
        # Symmetric: a new B pairs with every A already known for this M.
        if b not in self._outgoing[client_id][m]:
            self._outgoing[client_id][m].add(b)
            for a in self._incoming[client_id][m]:
                self._emit_relation(client_id, a, m, b)

    def _emit_relation(self, client_id: int, a: str, m: str, b: str):
        # Callers guarantee (A, M, B) is fresh (see the guards above), so there
        # is no dedup here: every call is a distinct relation.
        partition = partition_for_pair(a, b, SG_DETECTOR_AMOUNT)
        relation = ScatterGatherRelation(
            from_account=a,
            intermediate_account=m,
            to_account=b,
        )
        self._append_relation(client_id, partition, relation)
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
        batch_payload = self._batcher.append((client_id, partition), payload)
        if batch_payload is not None:
            self._send_batch(client_id, partition, batch_payload)

    def _send_batch(
        self, client_id: int, partition: int, batch_payload: bytes
    ) -> None:
        msg = self._proto.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=batch_payload,
        )
        self._detectors[partition].send(msg)
        logging.debug(
            "linker_%s batch_flush | client_id=%s | detector=%s | bytes=%s",
            ID, client_id, partition, len(msg),
        )

    def _flush_client(self, client_id: int) -> None:
        for (_, partition), batch_payload in self._batcher.flush(
            lambda k: k[0] == client_id
        ):
            self._send_batch(client_id, partition, batch_payload)

    def _handle_eof(self, client_id: int, payload: bytes):
        control_message = self._control_serializer.deserialize(payload)
        if control_message.sender_id in self._eofs_by_client[client_id]:
            logging.info(
                "linker_%s duplicate_eof | client_id=%s | mapper_id=%s",
                ID,
                client_id,
                control_message.sender_id,
            )
            return

        self._eofs_by_client[client_id].add(control_message.sender_id)
        logging.info(
            "linker_%s eof_received | client_id=%s | mapper_id=%s | "
            "eof_count=%s | mapper_expected_total=%s",
            ID,
            client_id,
            control_message.sender_id,
            len(self._eofs_by_client[client_id]),
            control_message.expected_total,
        )
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
        self._emitted_count_by_client.pop(client_id, None)
        self._emitted_count_by_partition.pop(client_id, None)
        self._data_batches_by_client.pop(client_id, None)
        self._edges_received_by_client.pop(client_id, None)
        self._batcher.discard(lambda k: k[0] == client_id)
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
