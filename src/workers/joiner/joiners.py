import logging
import os
import threading

from common import middleware
from common.constants import C_Q2, C_Q3, C_Q5
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import PersistentStateHandler
from common.fault_tolerance.inbox import MsgKind
from common.logging_utils import should_log_progress
from common.message_protocol.internal.common import MessageType
from common.message_protocol.internal.common.control_message import ControlMessage
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import InternalProtocol
from common.middleware.middleware_rabbitmq import ensure_exchange_queue_bindings
from common.routing import queue_name_for_worker

try:
    from processors import create_joiner_processor
    from joiner_state import JoinerState
except ImportError:
    from workers.joiner.processors import create_joiner_processor
    from workers.joiner.joiner_state import JoinerState


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
CONFIGURATION = os.environ["CONFIGURATION"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/joiner_state")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))

# Q3 output routes by client_id to the barrier shard that also receives that
# client's candidates.
Q3_AVERAGES_EXCHANGE = os.environ["Q3_AVERAGES_EXCHANGE"] if CONFIGURATION == C_Q3 else ""
Q3_AVERAGES_ROUTING_PREFIX = os.getenv(
    "Q3_AVERAGES_ROUTING_PREFIX", "q3_averages"
)
Q3_BARRIER_AMOUNT = int(os.environ["Q3_BARRIER_AMOUNT"]) if CONFIGURATION == C_Q3 else 0



class JoinerWorker:
    def __init__(self):
        if CONFIGURATION not in (C_Q2, C_Q3, C_Q5):
            raise ValueError(f"Invalid joiner configuration: {CONFIGURATION}")

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_resource = self._build_output(CONFIGURATION)

        self.internal_protocol = InternalProtocol()
        self.control_serializer = ControlMessageSerializer()

        self.lock = threading.Lock()
        self.closed = False
        self._state = JoinerState(lambda: create_joiner_processor(CONFIGURATION))
        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"joiner_{CONFIGURATION.lower()}_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
        )
        self._handler.recover()
        self._republish_pending()

    @property
    def processors_by_client(self):
        return self._state._processors_by_client

    @property
    def data_count_by_client(self):
        return self._state._data_count_by_client

    @property
    def eof_count_by_client(self):
        return self._state._eof_count_by_client

    @property
    def closed_by_client(self):
        return self._state._closed_by_client

    def _republish_pending(self) -> None:
        for entry in self._handler.outbox_to_republish():
            try:
                self.output_resource.send(entry.body)
            except Exception:
                logging.exception(
                    "joiner_republish_error | configuration=%s | id=%s | destination=%s",
                    CONFIGURATION,
                    ID,
                    entry.destination,
                )
                raise

    def _publish_commit(self, instruction) -> bool:
        if instruction.action is Action.ACK:
            return False
        for entry in instruction.outputs:
            self.output_resource.send(entry.body)
        with self.lock:
            self._handler.commit_done(*instruction.ctx)
        return True

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self.internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _output_packet(
        self, msg_type: MessageType, client_id: int, seq: int, payload: bytes
    ) -> bytes:
        # All downstream consumers (q2_bank_name_joiner, q3_barrier, gateway Q5)
        # dedup by (sender_id, seq), so always use addressed packets.
        return self.internal_protocol.create_addressed_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            sender_id=ID,
            seq=seq,
            payload=payload,
        )

    def _result_outputs(self, client_id: int, payloads: list[bytes]) -> list:
        outputs = []
        for seq, payload in enumerate(payloads):
            outputs.append(
                (
                    OUTPUT_QUEUE,
                    self._output_packet(MessageType.DATA, client_id, seq, payload),
                )
            )

        control_payload = self.control_serializer.serialize(
            ControlMessage(
                sender_id=ID, expected_total=len(payloads), processed_count=0
            )
        )
        outputs.append(
            (
                OUTPUT_QUEUE,
                self._output_packet(
                    MessageType.EOF, client_id, len(payloads), control_payload
                ),
            )
        )
        return outputs

    def _log_emit(self, client_id: int, results_count: int) -> None:
        logging.info(
            "joiner_emit | configuration=%s | id=%s | client_id=%s | results=%s",
            CONFIGURATION, ID, client_id, results_count,
        )

    def _build_output(self, configuration: str):
        if configuration == C_Q3:
            return middleware.ShardedByClientPublisher(
                MOM_HOST,
                Q3_AVERAGES_EXCHANGE,
                Q3_AVERAGES_ROUTING_PREFIX,
                Q3_BARRIER_AMOUNT,
            )
        return middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    def _ensure_output_bindings(self) -> None:
        if CONFIGURATION != C_Q3:
            return
        ensure_exchange_queue_bindings(
            MOM_HOST,
            Q3_AVERAGES_EXCHANGE,
            {
                queue_name_for_worker(Q3_AVERAGES_ROUTING_PREFIX, index): (
                    queue_name_for_worker(Q3_AVERAGES_ROUTING_PREFIX, index)
                )
                for index in range(Q3_BARRIER_AMOUNT)
            },
        )

    def _process_message(self, message: bytes) -> None:
        msg_type, client_id, sender_id, seq, payload = (
            self.internal_protocol.unpack_addressed_packet(message)
        )
        msg_type = MessageType(msg_type)
        msg_id = f"d:{sender_id}:{client_id}:{seq}"

        with self.lock:
            if msg_type == MessageType.DATA:
                def bfn(pl):
                    return JoinerState.data_change(client_id, pl), []

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, payload, bfn
                )
                data_count = self._state.data_count(client_id)
                should_emit = False
                emit_count = 0
                if should_log_progress(data_count):
                    logging.info(
                        "joiner_data | configuration=%s | id=%s | client_id=%s | "
                        "data_count=%s | payload_bytes=%s",
                        CONFIGURATION,
                        ID,
                        client_id,
                        data_count,
                        len(payload),
                    )

            elif msg_type == MessageType.EOF:
                def bfn(_pl):
                    if self._state.is_closed(client_id):
                        return JoinerState.eof_change(client_id), []
                    count_after_eof = self._state.eof_count(client_id) + 1
                    if count_after_eof == AGGREGATION_AMOUNT:
                        payloads = self._state.results_for(client_id)
                        return (
                            JoinerState.close_change(client_id),
                            self._result_outputs(client_id, payloads),
                        )
                    return JoinerState.eof_change(client_id), []

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, payload, bfn
                )
                count = self._state.eof_count(client_id)
                should_emit = bool(instruction.outputs)
                emit_count = max(len(instruction.outputs) - 1, 0)
                logging.info(
                    "joiner_eof_received | configuration=%s | id=%s | "
                    "client_id=%s | eof_count=%s | expected_eofs=%s | "
                    "ready=%s",
                    CONFIGURATION,
                    ID,
                    client_id,
                    count,
                    AGGREGATION_AMOUNT,
                    should_emit,
                )
            elif msg_type == MessageType.ABORT:
                abort_msg_id = f"abort:{sender_id}:{client_id}:{seq}"
                def bfn(_pl):
                    if CONFIGURATION == C_Q5:
                        return JoinerState.abort_change(client_id), []
                    abort_pkt = self._output_packet(MessageType.ABORT, client_id, 0, b"")
                    return JoinerState.abort_change(client_id), [(OUTPUT_QUEUE, abort_pkt)]
                instruction = self._handler.handle(
                    abort_msg_id, client_id, sender_id, seq, b"", bfn,
                    kind=MsgKind.ABORT,
                )

            else:
                raise ValueError(f"unsupported message type: {msg_type}")

        committed = self._publish_commit(instruction)
        if msg_type == MessageType.EOF and committed and should_emit:
            self._log_emit(client_id, emit_count)

    def process_messages(self, message, ack, nack):
        try:
            self._process_message(message)
            ack()
        except Exception as e:
            logging.error(
                "joiner_callback_error | configuration=%s | id=%s | error=%s",
                CONFIGURATION, ID, e,
            )
            nack(requeue=True)

    def start(self):
        self._ensure_output_bindings()
        logging.info(
            "joiner_start | configuration=%s | id=%s | input=%s | output=%s | "
            "aggregation_amount=%s",
            CONFIGURATION, ID, INPUT_QUEUE, OUTPUT_QUEUE, AGGREGATION_AMOUNT,
        )
        try:
            self.input_queue.start_consuming(self.process_messages)
        except Exception as e:
            logging.error(
                "joiner_start_error | configuration=%s | id=%s | error=%s",
                CONFIGURATION, ID, e,
            )
        finally:
            self.handle_sigterm()
            self.close()

    def handle_sigterm(self):
        logging.info(
            "joiner_shutdown | configuration=%s | id=%s", CONFIGURATION, ID
        )
        try:
            self.input_queue.stop_consuming()
        except Exception as e:
            logging.warning(
                "joiner_stop_input_error | id=%s | error=%s", ID, e
            )

    def close(self):
        if self.closed:
            return
        self.closed = True
        logging.info("joiner_close | configuration=%s | id=%s", CONFIGURATION, ID)
        for resource in (self.input_queue, self.output_resource):
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "joiner_close_error | id=%s | error=%s", ID, e
                )
