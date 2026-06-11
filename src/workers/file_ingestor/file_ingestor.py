import logging
import threading
import time
from dataclasses import dataclass

from common.domain.transaction import Transaction
from common.eof_coordinator import EofCoordinator, BroadcastAction, FlushAction, SendAnswerAction
from common.logging_utils import should_log_progress
from common.message_protocol.internal import InternalProtocol, LineBatchSerializer
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal.transaction_serializer import TransactionSerializer
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ
from workers.file_ingestor.line_batch_parser import LineBatchParser


@dataclass(frozen=True)
class FileIngestorConfig:
    id: int
    total_instances: int
    mom_host: str
    queue_name: str
    transaction_output_exchange: str
    control_queue_prefix: str
    response_queue_prefix: str
    logging_level: str


class FileIngestor:
    def __init__(self, config: FileIngestorConfig) -> None:
        self._config = config

        self._coordinator = EofCoordinator(
            instance_id=config.id,
            total_instances=config.total_instances,
            control_queue_prefix=config.control_queue_prefix,
            response_queue_prefix=config.response_queue_prefix,
            mode="broadcast",
        )

        self._internal_protocol = InternalProtocol()
        self._transaction_serializer = TransactionSerializer()
        self._line_batch_serializer = LineBatchSerializer()
        self._control_serializer = ControlMessageSerializer()

        self._input_queue: MessageMiddlewareQueueRabbitMQ | None = None
        self._transaction_output: MessageMiddlewareExchangeRabbitMQ | None = None
        # Named control queue senders for the main thread (EOF_RECEIVED broadcast).
        # Pika connections are not thread-safe; each thread creates its own senders.
        self._main_control_senders: dict = self._new_control_senders()

        # Set by the spawned threads before blocking on start_consuming.
        # Protected by _stopped_lock to avoid the race where stop() fires before
        # the thread assigns these fields.
        self._stopped_lock = threading.Lock()
        self._control_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._response_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._control_thread: threading.Thread | None = None
        self._response_thread: threading.Thread | None = None
        self._closed = False
        self._stopped = False

        self._lock = threading.Lock()
        self._processed_by_client: dict[int, int] = {}
        self._batches_consumed = 0

    # ---------- connection factories ----------

    def _new_control_senders(self) -> dict:
        return {
            self._coordinator.control_queue_for(i): MessageMiddlewareQueueRabbitMQ(
                self._config.mom_host, self._coordinator.control_queue_for(i)
            )
            for i in range(self._config.total_instances)
        }

    def _new_response_senders(self) -> dict:
        return {
            self._coordinator.response_queue_for(i): MessageMiddlewareQueueRabbitMQ(
                self._config.mom_host, self._coordinator.response_queue_for(i)
            )
            for i in range(self._config.total_instances)
        }

    def _new_transaction_sender(self) -> MessageMiddlewareExchangeRabbitMQ:
        return MessageMiddlewareExchangeRabbitMQ(
            host=self._config.mom_host,
            exchange_name=self._config.transaction_output_exchange,
            routing_keys=[],
            exchange_type="fanout",
        )

    def _transaction_sender(self) -> MessageMiddlewareExchangeRabbitMQ:
        if self._transaction_output is None:
            self._transaction_output = self._new_transaction_sender()
        return self._transaction_output

    # ---------- helpers ----------

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _eof_payload(self, expected_total: int) -> bytes:
        return self._control_serializer.serialize(
            ControlMessage(
                sender_id=self._config.id,
                expected_total=expected_total,
                processed_count=0,
            )
        )

    def _send_transaction_batch(
        self,
        sender: MessageMiddlewareExchangeRabbitMQ,
        client_id: int,
        transactions: list[Transaction],
    ) -> None:
        sender.send(
            self._packet(
                MessageType.DATA,
                client_id,
                self._transaction_serializer.serialize_batch(transactions),
            )
        )

    def _forward_eof_downstream(
        self,
        sender: MessageMiddlewareExchangeRabbitMQ,
        client_id: int,
        total_forwarded: int,
    ) -> None:
        sender.send(
            self._packet(MessageType.EOF, client_id, self._eof_payload(total_forwarded))
        )
        logging.info(
            "file_ingestor_eof_forwarded | id=%s | client_id=%s | total_fwd=%s",
            self._config.id, client_id, total_forwarded,
        )

    def _do_broadcast(self, action: BroadcastAction, control_senders: dict) -> None:
        if action.sleep_before > 0:
            time.sleep(action.sleep_before)
        for qname in action.queue_names:
            control_senders[qname].send(action.message)

    # ---------- data path ----------

    def _handle_line_batch(self, client_id: int, payload: bytes) -> None:
        batch = self._line_batch_serializer.deserialize(payload)
        transactions = LineBatchParser.parse(batch)

        if transactions:
            self._send_transaction_batch(
                self._transaction_sender(), client_id, transactions
            )

        forwarded = len(transactions)
        with self._lock:
            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + forwarded
            )
            self._batches_consumed += 1
            batch_count = self._batches_consumed

        if should_log_progress(batch_count):
            logging.info(
                "file_ingestor_line_batch | id=%s | client_id=%s | batches=%s",
                self._config.id, client_id, batch_count,
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
            "file_ingestor_upstream_eof | id=%s | client_id=%s | "
            "expected_total=%s | local_count=%s",
            self._config.id, client_id, expected_total, count,
        )

        if isinstance(action, BroadcastAction):
            self._do_broadcast(action, self._main_control_senders)

        elif isinstance(action, FlushAction):
            # N==1: coordinator returns total_forwarded=current_forwarded directly
            with self._lock:
                self._processed_by_client.pop(client_id, None)
            self._forward_eof_downstream(
                self._transaction_sender(), client_id, action.total_forwarded
            )

    def _process_message(self, message: bytes, ack, nack) -> None:
        try:
            if not message:
                raise ValueError("empty file ingestor message")
            msg_type, client_id, payload = self._internal_protocol.unpack_packet(message)
            if msg_type == MessageType.DATA:
                self._handle_line_batch(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, payload)
            else:
                raise ValueError(f"unknown file ingestor message type: {msg_type}")
            ack()
        except Exception as e:
            logging.error("file_ingestor_message_error | id=%s | error=%s", self._config.id, e)
            nack()

    # ---------- control path ----------

    def _handle_control(self, message: bytes, ack, nack, response_senders: dict) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("file_ingestor_control_parse_error | id=%s", self._config.id)
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
                    "file_ingestor_send_answer_error | id=%s | client_id=%s",
                    self._config.id, client_id,
                )
                with self._lock:
                    self._coordinator.clear_pending_eof(client_id)
                nack()

        elif isinstance(action, FlushAction):
            # Non-leader: send FLUSH_ACK with final forwarded count, then cleanup
            with self._lock:
                fwd_final = self._processed_by_client.get(client_id, 0)
            ack_msg = self._coordinator.build_flush_ack(client_id, fwd_final)
            try:
                response_senders[action.ack_queue].send(ack_msg)
            except Exception:
                logging.exception(
                    "file_ingestor_flush_ack_error | id=%s | client_id=%s",
                    self._config.id, client_id,
                )
                nack()
                return
            with self._lock:
                self._processed_by_client.pop(client_id, None)
                self._coordinator.cleanup_client(client_id)
            ack()

        else:
            logging.warning(
                "file_ingestor_unexpected_control_action | id=%s | action=%s",
                self._config.id, action,
            )
            ack()

    def _run_control_consumer(self) -> None:
        consumer = MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host, self._coordinator.my_control_queue()
        )
        response_senders = self._new_response_senders()

        # Register before blocking so stop() can reach us even if called concurrently.
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
            return

        try:
            consumer.start_consuming(
                lambda msg, ack, nack: self._handle_control(msg, ack, nack, response_senders)
            )
        except Exception as e:
            if not self._closed:
                logging.error(
                    "file_ingestor_control_consumer_stopped | id=%s | error=%s",
                    self._config.id, e,
                )
        finally:
            for q in response_senders.values():
                try:
                    q.close()
                except Exception:
                    pass
            try:
                consumer.close()
            except Exception:
                pass

    # ---------- response path (líder) ----------

    def _handle_response(
        self, message: bytes, ack, nack, control_senders: dict, eof_sender
    ) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("file_ingestor_response_parse_error | id=%s", self._config.id)
            nack()
            return

        own_fwd = 0
        with self._lock:
            action = self._coordinator.process_control_message(msg_type, client_id, ctrl)
            if isinstance(action, FlushAction) and action.is_leader:
                # Grab own forwarded under the same lock before cleanup.
                # Coordinator cleans its own state in _on_flush_ack.
                own_fwd = self._processed_by_client.pop(client_id, 0)

        if action is None:
            ack()
            return

        if isinstance(action, BroadcastAction):
            try:
                self._do_broadcast(action, control_senders)
                ack()
            except Exception:
                logging.exception(
                    "file_ingestor_broadcast_error | id=%s | client_id=%s",
                    self._config.id, client_id,
                )
                nack()

        elif isinstance(action, FlushAction) and action.is_leader:
            total_fwd = action.total_forwarded + own_fwd
            try:
                self._forward_eof_downstream(eof_sender, client_id, total_fwd)
                ack()
            except Exception:
                logging.exception(
                    "file_ingestor_forward_eof_error | id=%s | client_id=%s",
                    self._config.id, client_id,
                )
                nack()

        else:
            logging.warning(
                "file_ingestor_unexpected_response_action | id=%s | action=%s",
                self._config.id, action,
            )
            ack()

    def _run_response_consumer(self) -> None:
        consumer = MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host, self._coordinator.my_response_queue()
        )
        control_senders = self._new_control_senders()
        eof_sender = self._new_transaction_sender()

        # Register before blocking so stop() can reach us even if called concurrently.
        with self._stopped_lock:
            self._response_consumer = consumer
            already_stopped = self._stopped

        if already_stopped:
            for q in control_senders.values():
                try:
                    q.close()
                except Exception:
                    pass
            try:
                eof_sender.close()
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
                    msg, ack, nack, control_senders, eof_sender
                )
            )
        except Exception as e:
            if not self._closed:
                logging.error(
                    "file_ingestor_response_consumer_stopped | id=%s | error=%s",
                    self._config.id, e,
                )
        finally:
            for q in control_senders.values():
                try:
                    q.close()
                except Exception:
                    pass
            try:
                eof_sender.close()
            except Exception:
                pass
            try:
                consumer.close()
            except Exception:
                pass

    # ---------- lifecycle ----------

    def start(self) -> None:
        logging.info(
            "file_ingestor_start | id=%s | mom_host=%s | queue=%s | "
            "control_prefix=%s | response_prefix=%s | total_instances=%s",
            self._config.id,
            self._config.mom_host,
            self._config.queue_name,
            self._config.control_queue_prefix,
            self._config.response_queue_prefix,
            self._config.total_instances,
        )

        self._control_thread = threading.Thread(target=self._run_control_consumer)
        self._response_thread = threading.Thread(target=self._run_response_consumer)
        self._control_thread.start()
        self._response_thread.start()

        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host, self._config.queue_name
        )
        try:
            if not self._stopped:
                self._input_queue.start_consuming(self._process_message)
        finally:
            self.stop()
            if self._control_thread is not None:
                self._control_thread.join(timeout=5)
            if self._response_thread is not None:
                self._response_thread.join(timeout=5)
            self._close()

    def stop(self) -> None:
        with self._stopped_lock:
            if self._stopped:
                return
            self._stopped = True
            control_consumer = self._control_consumer
            response_consumer = self._response_consumer

        logging.info("file_ingestor_stop | id=%s", self._config.id)
        for consumer in (self._input_queue, control_consumer, response_consumer):
            if consumer is not None:
                consumer.request_stop_consuming()

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        resources = (
            [self._input_queue, self._transaction_output]
            + list(self._main_control_senders.values())
        )
        for resource in resources:
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "file_ingestor_close_error | id=%s | error=%s", self._config.id, e
                )
