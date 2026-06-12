import logging
import os
import threading
import time

from common import middleware
from common.constants import C_Q2, C_Q3, C_Q5
from common.eof_coordinator import EofCoordinator, BroadcastAction, FlushAction, SendAnswerAction
from common.logging_utils import should_log_progress
from common.message_protocol.internal.common import MessageType
from common.message_protocol.internal.common.control_message import ControlMessage
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import InternalProtocol
from common.middleware import MessageMiddlewareQueueRabbitMQ

try:
    from processors import create_aggregator_processor
except ImportError:
    from workers.aggregator.processors import create_aggregator_processor


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
CONFIGURATION = os.environ["CONFIGURATION"]
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
LEADER_ID = 0
AGGREGATION_CONTROL_QUEUE_PREFIX = f"{AGGREGATION_PREFIX}_control"
AGGREGATION_RESPONSE_QUEUE_PREFIX = f"{AGGREGATION_PREFIX}_response"


class AggregatorWorker:
    def __init__(self):
        if CONFIGURATION not in (C_Q2, C_Q3, C_Q5):
            raise ValueError(f"Invalid aggregator configuration: {CONFIGURATION}")

        self._coordinator = EofCoordinator(
            instance_id=ID,
            total_instances=AGGREGATION_AMOUNT,
            control_queue_prefix=AGGREGATION_CONTROL_QUEUE_PREFIX,
            response_queue_prefix=AGGREGATION_RESPONSE_QUEUE_PREFIX,
            mode="flush_order",
            leader_id=LEADER_ID,
        )

        self._internal_protocol = InternalProtocol()
        self._control_serializer = ControlMessageSerializer()

        self._lock = threading.Lock()
        self._data_count_by_client: dict[int, int] = {}
        self._processors_by_client: dict = {}
        self._closed_by_client: set[int] = set()

        self._input_exchange = None
        self.control_consumer = None
        self.response_consumer = None
        self.control_thread = None
        self.response_thread = None
        self.closed = False
        self._stopped = False

    # ---------- helpers ----------

    def _packet(self, msg_type, client_id, payload):
        return self._internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _eof_payload(self, expected_total):
        return self._control_serializer.serialize(
            ControlMessage(sender_id=ID, expected_total=expected_total, processed_count=0)
        )

    def _do_broadcast(self, action, control_senders):
        if action.sleep_before > 0:
            time.sleep(action.sleep_before)
        for qname in action.queue_names:
            control_senders[qname].send(action.message)

    def _processor_for_client(self, client_id):
        return self._processors_by_client.setdefault(
            client_id, create_aggregator_processor(CONFIGURATION)
        )

    def _new_control_senders(self):
        return {
            self._coordinator.control_queue_for(i): MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, self._coordinator.control_queue_for(i)
            )
            for i in range(AGGREGATION_AMOUNT)
        }

    def _emit_results(self, client_id, processor, data_count, output_queue):
        payloads = processor.results() if processor is not None else []
        for payload in payloads:
            output_queue.send(self._packet(MessageType.DATA, client_id, payload))
        output_queue.send(
            self._packet(MessageType.EOF, client_id, self._eof_payload(len(payloads)))
        )
        logging.info(
            "aggregation_emit | configuration=%s | id=%s | client_id=%s | "
            "input_data=%s | results=%s",
            CONFIGURATION, ID, client_id, data_count, len(payloads),
        )

    # ---------- data path ----------

    def _handle_data(self, client_id, payload):
        with self._lock:
            if client_id in self._closed_by_client:
                return
            self._processor_for_client(client_id).accept(payload)
            self._data_count_by_client[client_id] = (
                self._data_count_by_client.get(client_id, 0) + 1
            )
            data_count = self._data_count_by_client[client_id]
        if should_log_progress(data_count):
            logging.info(
                "aggregation_data | configuration=%s | id=%s | client_id=%s | "
                "data_count=%s | payload_bytes=%s",
                CONFIGURATION, ID, client_id, data_count, len(payload),
            )

    def _handle_upstream_eof(self, client_id, payload, response_queue, output_queue):
        ctrl = self._control_serializer.deserialize(payload)
        with self._lock:
            if client_id in self._closed_by_client:
                return
            count = self._data_count_by_client.get(client_id, 0)
            action = self._coordinator.on_upstream_eof(
                client_id, ctrl.expected_total, count, 0
            )
        if action is None:
            return
        logging.info(
            "aggregation_upstream_eof | configuration=%s | id=%s | client_id=%s | "
            "data_count=%s | expected_total=%s",
            CONFIGURATION, ID, client_id, count, ctrl.expected_total,
        )
        if isinstance(action, SendAnswerAction):
            response_queue.send(action.message)
        elif isinstance(action, FlushAction):
            # N==1 shortcut: flush directly from data thread
            with self._lock:
                processor = self._processors_by_client.pop(client_id, None)
                data_count = self._data_count_by_client.pop(client_id, 0)
                self._closed_by_client.add(client_id)
            self._emit_results(client_id, processor, data_count, output_queue)

    def _process_data_message(self, message, ack, nack, response_queue, output_queue):
        try:
            msg_type, client_id, payload = self._internal_protocol.unpack_packet(message)
            if msg_type == MessageType.DATA:
                self._handle_data(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, payload, response_queue, output_queue)
            else:
                raise ValueError(f"unsupported aggregator message type: {msg_type}")
            ack()
        except Exception:
            logging.exception(
                "aggregation_data_error | configuration=%s | id=%s", CONFIGURATION, ID
            )
            nack()

    # ---------- control path ----------

    def _handle_control(self, message, ack, nack, response_sender, output_queue):
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception(
                "aggregation_control_parse_error | configuration=%s | id=%s",
                CONFIGURATION, ID,
            )
            nack()
            return

        processor = data_count = None
        with self._lock:
            count = self._data_count_by_client.get(client_id, 0)
            action = self._coordinator.process_control_message(
                msg_type, client_id, ctrl, count, 0
            )
            if isinstance(action, FlushAction):
                if client_id in self._closed_by_client:
                    ack()
                    return
                processor = self._processors_by_client.pop(client_id, None)
                data_count = self._data_count_by_client.pop(client_id, 0)
                self._closed_by_client.add(client_id)
                self._coordinator.cleanup_client(client_id)

        if action is None:
            ack()
            return
        if isinstance(action, SendAnswerAction):
            response_sender.send(action.message)
            ack()
        elif isinstance(action, FlushAction):
            self._emit_results(client_id, processor, data_count, output_queue)
            response_sender.send(self._coordinator.build_flush_ack(client_id, 0))
            ack()
        else:
            logging.warning(
                "aggregation_unexpected_control_action | id=%s | action=%s", ID, action
            )
            ack()

    def _start_control_consumer(self):
        # Assign before the _stopped check so handle_sigterm can always reach this consumer.
        control_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_control_queue()
        )
        self.control_consumer = control_consumer
        response_sender = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.response_queue_for(LEADER_ID)
        )
        output_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        try:
            if not self._stopped:
                control_consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_control(
                        msg, ack, nack, response_sender, output_queue
                    )
                )
        finally:
            response_sender.close()
            output_queue.close()
            control_consumer.close()

    # ---------- response path (leader only) ----------

    def _handle_response(self, message, ack, nack, control_senders, output_queue):
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception(
                "aggregation_response_parse_error | configuration=%s | id=%s",
                CONFIGURATION, ID,
            )
            nack()
            return

        processor = data_count = None
        with self._lock:
            action = self._coordinator.process_control_message(msg_type, client_id, ctrl)
            if isinstance(action, FlushAction) and action.is_leader:
                if client_id in self._closed_by_client:
                    ack()
                    return
                processor = self._processors_by_client.pop(client_id, None)
                data_count = self._data_count_by_client.pop(client_id, 0)
                self._closed_by_client.add(client_id)

        if action is None:
            ack()
            return
        if isinstance(action, BroadcastAction):
            self._do_broadcast(action, control_senders)
            ack()
        elif isinstance(action, FlushAction) and action.is_leader:
            self._emit_results(client_id, processor, data_count, output_queue)
            ack()
        else:
            logging.warning(
                "aggregation_unexpected_response_action | id=%s | action=%s", ID, action
            )
            ack()

    def _start_response_consumer(self):
        # Assign before the _stopped check so handle_sigterm can always reach this consumer.
        response_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_response_queue()
        )
        self.response_consumer = response_consumer
        control_senders = self._new_control_senders()
        output_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        try:
            if not self._stopped:
                response_consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_response(
                        msg, ack, nack, control_senders, output_queue
                    )
                )
        finally:
            for q in control_senders.values():
                q.close()
            output_queue.close()
            response_consumer.close()

    # ---------- lifecycle ----------

    def start(self):
        logging.info(
            "aggregation_start | configuration=%s | id=%s | exchange=%s | "
            "output_queue=%s | leader=%s | total=%s",
            CONFIGURATION, ID, f"{AGGREGATION_PREFIX}_{ID}", OUTPUT_QUEUE,
            ID == LEADER_ID, AGGREGATION_AMOUNT,
        )
        self.control_thread = threading.Thread(target=self._start_control_consumer)
        self.control_thread.start()
        if self._coordinator.needs_response_consumer():
            self.response_thread = threading.Thread(target=self._start_response_consumer)
            self.response_thread.start()

        # Data-thread connections: response_queue for EOF reports, output for N==1 flush.
        data_response = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.response_queue_for(LEADER_ID)
        )
        data_output = MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self._input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        try:
            if not self._stopped:
                self._input_exchange.start_consuming(
                    lambda msg, ack, nack: self._process_data_message(
                        msg, ack, nack, data_response, data_output
                    )
                )
        finally:
            data_response.close()
            data_output.close()
            self.handle_sigterm()
            if self.control_thread is not None:
                self.control_thread.join(timeout=5)
            if self.response_thread is not None:
                self.response_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self):
        if self._stopped:
            return
        self._stopped = True
        logging.info(
            "aggregation_shutdown | configuration=%s | id=%s", CONFIGURATION, ID
        )
        self._input_exchange.request_stop_consuming()
        if self.control_consumer is not None:
            self.control_consumer.request_stop_consuming()
        if self.response_consumer is not None:
            self.response_consumer.request_stop_consuming()

    def close(self):
        if self.closed:
            return
        self.closed = True
        logging.info("aggregation_close | configuration=%s | id=%s", CONFIGURATION, ID)
        try:
            self._input_exchange.close()
        except Exception as e:
            logging.warning("aggregation_close_error | id=%s | error=%s", ID, e)
