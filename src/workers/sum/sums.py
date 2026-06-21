import logging
import os
import threading
import time
import zlib

from common import middleware
from common.constants import C_Q2, C_Q3
from common.domain.transaction import Transaction
from common.eof_coordinator import EofCoordinator, BroadcastAction, FlushAction, SendAnswerAction
from common.logging_utils import should_log_progress
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import InternalProtocol
from common.message_protocol.internal.transaction_serializer import TransactionSerializer
from common.middleware import LazyQueue, MessageMiddlewareQueueRabbitMQ
from common.routing import queue_name_for_worker

try:
    from processors import create_sum_processor
except ImportError:
    from workers.sum.processors import create_sum_processor


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
CONFIGURATION = os.getenv("CONFIGURATION", C_Q2)
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
if CONFIGURATION in (C_Q2, C_Q3):
    INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
    INPUT_ROUTING_PREFIX = os.environ["INPUT_ROUTING_PREFIX"]
else:
    INPUT_EXCHANGE = os.getenv("INPUT_EXCHANGE")
    INPUT_ROUTING_PREFIX = os.getenv("INPUT_ROUTING_PREFIX")
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_QUEUE_PREFIX = f"{SUM_PREFIX}_control"
SUM_RESPONSE_QUEUE_PREFIX = f"{SUM_PREFIX}_response"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumWorker:
    def __init__(self):
        if CONFIGURATION not in (C_Q2, C_Q3):
            raise ValueError(f"Invalid sum configuration: {CONFIGURATION}")

        self._coordinator = EofCoordinator(
            instance_id=ID,
            total_instances=SUM_AMOUNT,
            control_queue_prefix=SUM_CONTROL_QUEUE_PREFIX,
            response_queue_prefix=SUM_RESPONSE_QUEUE_PREFIX,
            mode="broadcast",
        )

        self._internal_protocol = InternalProtocol()
        self._transaction_serializer = TransactionSerializer()
        self._control_serializer = ControlMessageSerializer()

        self._lock = threading.Lock()
        self._processed_by_client: dict[int, int] = {}
        self._processors_by_client: dict = {}
        self._partials_forwarded = 0
        # In-memory monotonic seq counter per client for addressed packets sent to
        # the aggregator exchange.  Resets to 0 on restart; the aggregator's inbox
        # treats any seq it has already marked DONE as a duplicate and ignores it,
        # so a post-restart sequence collision is safe.
        self._agg_seq_by_client: dict[int, int] = {}

        # Named control queue senders for the main thread (EOF_RECEIVED broadcast).
        # Pika connections are not thread-safe; each thread creates its own senders.
        self._main_control_senders: dict = self._new_control_senders()
        self._output_exchanges = self._new_output_exchanges()

        # Set by the spawned threads before blocking on start_consuming.
        # Protected by _stopped_lock to avoid the race where handle_sigterm fires before
        # the thread assigns these fields.
        self._stopped_lock = threading.Lock()
        self._input_queue: MessageMiddlewareQueueRabbitMQ | None = None
        self._control_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._response_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._control_thread: threading.Thread | None = None
        self._response_thread: threading.Thread | None = None
        self._closed = False
        self._stopped = False

    # ---------- connection factories ----------

    def _new_output_exchanges(self):
        return [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                AGGREGATION_PREFIX,
                [f"{AGGREGATION_PREFIX}_{i}"],
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

    def _new_control_senders(self) -> dict:
        return {
            self._coordinator.control_queue_for(i): LazyQueue(
                MOM_HOST, self._coordinator.control_queue_for(i)
            )
            for i in range(SUM_AMOUNT)
        }

    def _new_response_senders(self) -> dict:
        return {
            self._coordinator.response_queue_for(i): LazyQueue(
                MOM_HOST, self._coordinator.response_queue_for(i)
            )
            for i in range(SUM_AMOUNT)
        }

    # ---------- helpers ----------

    def _aggregation_index(self, partition_key: str) -> int:
        return zlib.crc32(partition_key.encode("utf-8")) % AGGREGATION_AMOUNT

    def _input_routing_key(self) -> str:
        if INPUT_ROUTING_PREFIX is None:
            raise RuntimeError("INPUT_ROUTING_PREFIX is required for sharded input")
        return queue_name_for_worker(INPUT_ROUTING_PREFIX, ID)

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _next_agg_seq(self, client_id: int) -> int:
        seq = self._agg_seq_by_client.get(client_id, 0)
        self._agg_seq_by_client[client_id] = (seq + 1) & 0xFFFFFFFF
        return seq

    def _addressed_packet(
        self, msg_type: MessageType, client_id: int, seq: int, payload: bytes
    ) -> bytes:
        return self._internal_protocol.create_addressed_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            sender_id=ID,
            seq=seq,
            payload=payload,
        )

    def _eof_payload(self, expected_total: int) -> bytes:
        return self._control_serializer.serialize(
            ControlMessage(
                sender_id=ID,
                expected_total=expected_total,
                processed_count=0,
            )
        )

    def _do_broadcast(self, action: BroadcastAction, control_senders: dict) -> None:
        if action.sleep_before > 0:
            time.sleep(action.sleep_before)
        for qname in action.queue_names:
            control_senders[qname].send(action.message)

    def _processor_for_client(self, client_id: int):
        return self._processors_by_client.setdefault(
            client_id,
            create_sum_processor(CONFIGURATION),
        )

    def _partials_for_client(self, client_id: int):
        processor = self._processors_by_client.get(client_id)
        if processor is None:
            return []
        return processor.partials()

    def _forward_partial(
        self,
        client_id: int,
        partition_key: str,
        payload: bytes,
        exchanges,
    ) -> None:
        index = self._aggregation_index(partition_key)
        seq = self._next_agg_seq(client_id)
        exchanges[index].send(self._addressed_packet(MessageType.DATA, client_id, seq, payload))
        self._partials_forwarded += 1
        if should_log_progress(self._partials_forwarded):
            logging.info(
                "sum_forward_partial | configuration=%s | id=%s | client_id=%s | "
                "partition_key=%s | aggregation_index=%s",
                CONFIGURATION, ID, client_id, partition_key, index,
            )

    def _forward_partials(self, client_id: int, partials, exchanges) -> int:
        forwarded = 0
        for partition_key, payload in partials:
            self._forward_partial(client_id, partition_key, payload, exchanges)
            forwarded += 1
        return forwarded

    def _forward_eof_to_aggregators(
        self, client_id: int, expected_total: int, exchanges
    ) -> None:
        payload = self._eof_payload(expected_total)
        for index, exchange in enumerate(exchanges):
            seq = self._next_agg_seq(client_id)
            exchange.send(self._addressed_packet(MessageType.EOF, client_id, seq, payload))
            logging.info(
                "sum_forward_eof_to_aggregator | configuration=%s | id=%s | "
                "client_id=%s | aggregation_index=%s | expected_total=%s",
                CONFIGURATION, ID, client_id, index, expected_total,
            )

    # ---------- data path ----------

    def _handle_data_packet(self, client_id: int, payload: bytes) -> None:
        transactions = self._transaction_serializer.deserialize_batch(payload)
        if not transactions:
            return

        with self._lock:
            for transaction in transactions:
                self._processor_for_client(client_id).process(transaction)
            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + len(transactions)
            )
            processed_total = self._processed_by_client[client_id]

        if should_log_progress(processed_total):
            logging.info(
                "sum_data_batch | configuration=%s | id=%s | client_id=%s | "
                "batch_size=%s | processed_total=%s",
                CONFIGURATION, ID, client_id, len(transactions), processed_total,
            )

    def _handle_upstream_eof(self, client_id: int, payload: bytes) -> None:
        ctrl = self._control_serializer.deserialize(payload)
        expected_total = ctrl.expected_total

        with self._lock:
            count = self._processed_by_client.get(client_id, 0)
            action = self._coordinator.on_upstream_eof(
                client_id, expected_total, count, count
            )

        logging.info(
            "sum_upstream_eof | configuration=%s | id=%s | client_id=%s | "
            "expected_total=%s | local_count=%s",
            CONFIGURATION, ID, client_id, expected_total, count,
        )

        if isinstance(action, BroadcastAction):
            self._do_broadcast(action, self._main_control_senders)

        elif isinstance(action, FlushAction):
            # N==1: flush own partials and send downstream EOF
            with self._lock:
                partials = self._partials_for_client(client_id)
                self._processors_by_client.pop(client_id, None)
                self._processed_by_client.pop(client_id, None)
            fwd_final = self._forward_partials(client_id, partials, self._output_exchanges)
            self._forward_eof_to_aggregators(client_id, fwd_final, self._output_exchanges)

    def _process_message(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, payload = self._internal_protocol.unpack_packet(message)
            if msg_type == MessageType.DATA:
                self._handle_data_packet(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, payload)
            else:
                raise ValueError(f"Unexpected sum data message type: {msg_type}")
            ack()
        except Exception:
            logging.exception("sum_data_error | configuration=%s | id=%s", CONFIGURATION, ID)
            nack()

    # ---------- control path ----------

    def _handle_control(
        self,
        message: bytes,
        ack,
        nack,
        response_senders: dict,
        output_exchanges,
    ) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception(
                "sum_control_parse_error | configuration=%s | id=%s", CONFIGURATION, ID
            )
            nack()
            return

        with self._lock:
            count = self._processed_by_client.get(client_id, 0)
            action = self._coordinator.process_control_message(
                msg_type, client_id, ctrl, count, count
            )

        if action is None:
            ack()
            return

        if isinstance(action, SendAnswerAction):
            try:
                response_senders[action.queue_name].send(action.message)
                ack()
            except Exception:
                logging.exception(
                    "sum_send_answer_error | configuration=%s | id=%s | client_id=%s",
                    CONFIGURATION, ID, client_id,
                )
                with self._lock:
                    self._coordinator.clear_pending_eof(client_id)
                nack()

        elif isinstance(action, FlushAction):
            # Non-leader: snapshot partials, forward, send FLUSH_ACK, then cleanup
            with self._lock:
                partials = self._partials_for_client(client_id)
            fwd_final = self._forward_partials(client_id, partials, output_exchanges)
            ack_msg = self._coordinator.build_flush_ack(client_id, fwd_final)
            try:
                response_senders[action.ack_queue].send(ack_msg)
            except Exception:
                logging.exception(
                    "sum_flush_ack_error | configuration=%s | id=%s | client_id=%s",
                    CONFIGURATION, ID, client_id,
                )
                nack()
                return
            with self._lock:
                self._processors_by_client.pop(client_id, None)
                self._processed_by_client.pop(client_id, None)
                self._coordinator.cleanup_client(client_id)
            logging.info(
                "sum_flush_ack_sent | configuration=%s | id=%s | client_id=%s | fwd=%s",
                CONFIGURATION, ID, client_id, fwd_final,
            )
            ack()

        else:
            logging.warning(
                "sum_unexpected_control_action | configuration=%s | id=%s | action=%s",
                CONFIGURATION, ID, action,
            )
            ack()

    def _run_control_consumer(self) -> None:
        consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_control_queue()
        )
        response_senders = self._new_response_senders()
        output_exchanges = self._new_output_exchanges()

        with self._stopped_lock:
            self._control_consumer = consumer
            already_stopped = self._stopped

        if already_stopped:
            try:
                consumer.close()
            except Exception:
                pass
            for q in response_senders.values():
                try:
                    q.close()
                except Exception:
                    pass
            for ex in output_exchanges:
                try:
                    ex.close()
                except Exception:
                    pass
            return

        try:
            consumer.start_consuming(
                lambda msg, ack, nack: self._handle_control(
                    msg, ack, nack, response_senders, output_exchanges
                )
            )
        except Exception as e:
            if not self._closed:
                logging.error(
                    "sum_control_consumer_stopped | configuration=%s | id=%s | error=%s",
                    CONFIGURATION, ID, e,
                )
        finally:
            for q in response_senders.values():
                try:
                    q.close()
                except Exception:
                    pass
            for ex in output_exchanges:
                try:
                    ex.close()
                except Exception:
                    pass
            try:
                consumer.close()
            except Exception:
                pass

    # ---------- response path (leader) ----------

    def _handle_response(
        self,
        message: bytes,
        ack,
        nack,
        control_senders: dict,
        output_exchanges,
    ) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception(
                "sum_response_parse_error | configuration=%s | id=%s", CONFIGURATION, ID
            )
            nack()
            return

        own_partials = []
        with self._lock:
            action = self._coordinator.process_control_message(msg_type, client_id, ctrl)
            if isinstance(action, FlushAction) and action.is_leader:
                own_partials = self._partials_for_client(client_id)
                self._processors_by_client.pop(client_id, None)
                self._processed_by_client.pop(client_id, None)

        if action is None:
            ack()
            return

        if isinstance(action, BroadcastAction):
            try:
                self._do_broadcast(action, control_senders)
                ack()
            except Exception:
                logging.exception(
                    "sum_broadcast_error | configuration=%s | id=%s | client_id=%s",
                    CONFIGURATION, ID, client_id,
                )
                nack()

        elif isinstance(action, FlushAction) and action.is_leader:
            own_fwd = self._forward_partials(client_id, own_partials, output_exchanges)
            total_fwd = action.total_forwarded + own_fwd
            logging.info(
                "sum_eof_ready | configuration=%s | id=%s | client_id=%s | "
                "total_fwd=%s | own_fwd=%s | acks_fwd=%s",
                CONFIGURATION, ID, client_id, total_fwd, own_fwd, action.total_forwarded,
            )
            try:
                self._forward_eof_to_aggregators(client_id, total_fwd, output_exchanges)
                ack()
            except Exception:
                logging.exception(
                    "sum_forward_eof_error | configuration=%s | id=%s | client_id=%s",
                    CONFIGURATION, ID, client_id,
                )
                nack()

        else:
            logging.warning(
                "sum_unexpected_response_action | configuration=%s | id=%s | action=%s",
                CONFIGURATION, ID, action,
            )
            ack()

    def _run_response_consumer(self) -> None:
        consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_response_queue()
        )
        control_senders = self._new_control_senders()
        output_exchanges = self._new_output_exchanges()

        with self._stopped_lock:
            self._response_consumer = consumer
            already_stopped = self._stopped

        if already_stopped:
            for q in control_senders.values():
                try:
                    q.close()
                except Exception:
                    pass
            for ex in output_exchanges:
                try:
                    ex.close()
                except Exception:
                    pass
            try:
                consumer.close()
            except Exception:
                pass
            return

        try:
            consumer.start_consuming(
                lambda msg, ack, nack: self._handle_response(
                    msg, ack, nack, control_senders, output_exchanges
                )
            )
        except Exception as e:
            if not self._closed:
                logging.error(
                    "sum_response_consumer_stopped | configuration=%s | id=%s | error=%s",
                    CONFIGURATION, ID, e,
                )
        finally:
            for q in control_senders.values():
                try:
                    q.close()
                except Exception:
                    pass
            for ex in output_exchanges:
                try:
                    ex.close()
                except Exception:
                    pass
            try:
                consumer.close()
            except Exception:
                pass

    # ---------- lifecycle ----------

    def start(self) -> None:
        input_kind = "exchange" if CONFIGURATION in (C_Q2, C_Q3) else "queue"
        logging.info(
            "sum_start | configuration=%s | id=%s | mom_host=%s | input=%s | "
            "input_kind=%s | "
            "sum_amount=%s | aggregation_amount=%s",
            CONFIGURATION, ID, MOM_HOST, INPUT_QUEUE, input_kind,
            SUM_AMOUNT, AGGREGATION_AMOUNT,
        )

        self._control_thread = threading.Thread(target=self._run_control_consumer)
        self._response_thread = threading.Thread(target=self._run_response_consumer)
        self._control_thread.start()
        self._response_thread.start()

        if CONFIGURATION in (C_Q2, C_Q3):
            self._input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                INPUT_EXCHANGE,
                [self._input_routing_key()],
                queue_name=INPUT_QUEUE,
                exclusive=False,
            )
        else:
            self._input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST,
                INPUT_QUEUE,
            )
        try:
            if not self._stopped:
                self._input_queue.start_consuming(self._process_message)
        finally:
            self.handle_sigterm()
            self._control_thread.join(timeout=5)
            self._response_thread.join(timeout=5)
            self._close()

    def handle_sigterm(self) -> None:
        with self._stopped_lock:
            if self._stopped:
                return
            self._stopped = True
            control_consumer = self._control_consumer
            response_consumer = self._response_consumer

        logging.info("sum_shutdown | configuration=%s | id=%s", CONFIGURATION, ID)
        for consumer in (self._input_queue, control_consumer, response_consumer):
            if consumer is not None:
                consumer.request_stop_consuming()

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True

        resources = (
            [self._input_queue]
            + list(self._main_control_senders.values())
            + self._output_exchanges
        )
        for resource in resources:
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "sum_close_error | configuration=%s | id=%s | error=%s",
                    CONFIGURATION, ID, e,
                )
