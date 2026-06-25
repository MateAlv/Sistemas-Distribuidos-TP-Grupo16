import logging
import threading
import time
from dataclasses import dataclass

from common.eof_coordinator import EofCoordinator, BroadcastAction, SendAnswerAction
from common.logging_utils import should_log_progress
from common.message_protocol.internal import InternalProtocol, LineBatchSerializer
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal.transaction_serializer import TransactionSerializer
from common.fault_tolerance.handler import PersistentStateHandler, WorkerRunner
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.sender_sequencer import EdgeSpec
from common.fault_tolerance.inbox import InboxStatus, MsgKind
from common.middleware import (
    LazyQueue,
    MessageMiddlewareQueueRabbitMQ,
    ShardedPublisher,
)
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
    ensure_exchange_queue_bindings,
)
from common.routing import queue_name_for_worker
from workers.file_ingestor.file_ingestor_state import FileIngestorState
from workers.file_ingestor.line_batch_parser import LineBatchParser


@dataclass(frozen=True)
class FileIngestorOutputConfig:
    name: str
    exchange: str
    routing_prefix: str
    shard_count: int


@dataclass(frozen=True)
class FileIngestorConfig:
    id: int
    total_instances: int
    mom_host: str
    queue_name: str
    input_exchange: str
    input_routing_prefix: str
    outputs: tuple[FileIngestorOutputConfig, ...]
    control_queue_prefix: str
    response_queue_prefix: str
    logging_level: str
    state_dir: str = ""
    snapshot_interval: int = 1000


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

        self._input_queue: MessageMiddlewareQueueRabbitMQ | MessageMiddlewareExchangeRabbitMQ | None = None
        self._downstream_outputs: dict | None = None
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

        # One lock serializes the handler (data thread) with every coordinator
        # access on the control/response threads, so the snapshot is consistent.
        self._lock = threading.Lock()

        self._state = FileIngestorState(self._coordinator)
        self._handler = PersistentStateHandler(
            state_dir=config.state_dir,
            node_id=f"file_ingestor_{config.id}",
            worker_state=self._state,
            snapshot_every=config.snapshot_interval,
            output_edges=self._output_edges(),
        )
        self._runner: WorkerRunner | None = None

    def _output_edges(self) -> dict[str, EdgeSpec]:
        """One addressed edge per filter output. The SenderSequencer stamps a
        durable per-(client, edge, shard) seq, so a downstream filter won't
        dedup-drop a batch after recovery."""
        return {
            output.name: EdgeSpec(self._config.id, output.shard_count)
            for output in self._config.outputs
        }

    def _new_control_senders(self) -> dict:
        return {
            self._coordinator.control_queue_for(i): LazyQueue(
                self._config.mom_host, self._coordinator.control_queue_for(i)
            )
            for i in range(self._config.total_instances)
        }

    def _new_response_senders(self) -> dict:
        return {
            self._coordinator.response_queue_for(i): LazyQueue(
                self._config.mom_host, self._coordinator.response_queue_for(i)
            )
            for i in range(self._config.total_instances)
        }

    def _new_downstream_senders(self) -> dict:
        """One ShardedPublisher per output edge. The SenderSequencer already
        addressed each packet and picked its shard; the publisher just sends it."""
        return {
            output.name: ShardedPublisher(
                self._config.mom_host,
                output.exchange,
                output.routing_prefix,
                output.shard_count,
            )
            for output in self._config.outputs
        }

    def _downstream_senders(self) -> dict:
        if self._downstream_outputs is None:
            self._downstream_outputs = self._new_downstream_senders()
        return self._downstream_outputs

    def _input_routing_key(self) -> str:
        return queue_name_for_worker(self._config.input_routing_prefix, self._config.id)

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

    def _downstream_eof_outputs(self, client_id: int, total_forwarded: int) -> list:
        """One downstream EOF per filter output, carrying the cross-replica
        forwarded total. Goes through the outbox, so recovery re-publishes it."""
        eof_payload = self._eof_payload(total_forwarded)
        return [
            (output.name, self._packet(MessageType.EOF, client_id, eof_payload))
            for output in self._config.outputs
        ]

    def _publish_outputs(self, entries, publishers: dict) -> None:
        """Publish outbox entries, resolving each destination against the publishers
        map. Sharded entries go to their shard; plain queue entries send as-is."""
        for entry in entries:
            publisher = publishers.get(entry.destination)
            if publisher is None:
                raise KeyError(f"no publisher for destination {entry.destination!r}")
            if entry.shard is None:
                publisher.send(entry.body)
            else:
                publisher.send_to_shard(entry.body, entry.shard)

    def _do_broadcast(self, action: BroadcastAction, control_senders: dict) -> None:
        if action.sleep_before > 0:
            time.sleep(action.sleep_before)
        for qname in action.queue_names:
            control_senders[qname].send(action.message)

    def _on_input_message(self, message: bytes, ack, nack) -> None:
        """Dispatch by type: DATA goes through the durable handler (runner);
        upstream EOF drives the coordinator (snapshot-durable)."""
        if not message:
            logging.error("file_ingestor_empty_message | id=%s", self._config.id)
            nack(requeue=False)
            return
        msg_type = message[0]
        if msg_type == MessageType.DATA:
            self._runner.process(message, ack, nack)
        elif msg_type == MessageType.EOF:
            self._handle_upstream_eof(message, ack, nack)
        elif msg_type == MessageType.ABORT:
            self._handle_abort(message, ack, nack)
        else:
            logging.error(
                "file_ingestor_unknown_message_type | id=%s | type=%s",
                self._config.id, msg_type,
            )
            nack(requeue=False)

    def _data_process_payload(self, client_id: int, payload: bytes):
        """Parse a DATA line batch: return the forwarded count (the state change)
        and one transaction batch per output. No state mutation here, so replay
        reproduces it. A closed client yields nothing."""
        if self._state.is_closed(client_id):
            return FileIngestorState.data_change(client_id, 0), []

        batch = self._line_batch_serializer.deserialize(payload)
        transactions = LineBatchParser.parse(batch)

        outputs = []
        if transactions:
            body = self._transaction_serializer.serialize_batch(transactions)
            packet = self._packet(MessageType.DATA, client_id, body)
            outputs = [(output.name, packet) for output in self._config.outputs]

        if should_log_progress(self._state.batches_consumed()):
            logging.info(
                "file_ingestor_line_batch | id=%s | client_id=%s | batches=%s",
                self._config.id, client_id, self._state.batches_consumed(),
            )

        return FileIngestorState.data_change(client_id, len(transactions)), outputs

    def _handle_upstream_eof(self, message: bytes, ack, nack) -> None:
        """Upstream EOF, handled as a durable input under CTRL_UPSTREAM_EOF (a
        separate inbox bucket so its (sender, seq) can't collide with a DATA
        packet). on_upstream_eof is idempotent, so replay is safe; the broadcast
        (or the N==1 downstream EOF) rides the outbox and is re-published on recovery."""
        try:
            _msg_type, client_id, sender_id, seq, payload = (
                self._internal_protocol.unpack_addressed_packet(message)
            )
            ctrl = self._control_serializer.deserialize(payload)
            expected_total = ctrl.expected_total
            msg_id = f"d:{sender_id}:{client_id}:{seq}"

            with self._lock:
                status = self._handler.inbox.classify(
                    client_id, sender_id, seq, MsgKind.CTRL_UPSTREAM_EOF
                )
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return
                count = self._state.processed_count(client_id)

                def bfn(_pl):
                    eof_change = FileIngestorState.coordinator_upstream_eof_change(
                        client_id, expected_total, count, count
                    )
                    if self._config.total_instances > 1:
                        queue_names = [
                            self._coordinator.control_queue_for(i)
                            for i in range(self._config.total_instances)
                        ]
                        message = self._packet(
                            MessageType.EOF_RECEIVED,
                            client_id,
                            self._eof_payload(expected_total),
                        )
                        return eof_change, [
                            (qname, message) for qname in queue_names
                        ]
                    # N==1: flush directly, no inter-replica coordination.
                    outputs = self._downstream_eof_outputs(client_id, count)
                    compound = FileIngestorState.compound_change(
                        eof_change, FileIngestorState.close_change(client_id)
                    )
                    return compound, outputs

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, payload, bfn,
                    kind=MsgKind.CTRL_UPSTREAM_EOF,
                )

            self._publish_outputs(instruction.outputs, self._data_publishers)
            with self._lock:
                self._handler.commit_done(*instruction.ctx)
            ack()
            logging.info(
                "file_ingestor_upstream_eof | id=%s | client_id=%s | "
                "expected_total=%s | local_count=%s",
                self._config.id, client_id, expected_total, count,
            )
        except Exception as e:
            logging.error(
                "file_ingestor_upstream_eof_error | id=%s | error=%s",
                self._config.id, e, exc_info=True,
            )
            nack(requeue=True)

    def _handle_abort(self, message: bytes, ack, nack) -> None:
        """Handle an ABORT tombstone received from file_splitter.

        WAL-tracked via MsgKind.ABORT (its own inbox bucket, sender=splitter_id,
        seq stamped by the upstream SenderSequencer). On recovery the outbox
        re-publishes the downstream ABORT packets so the tombstone survives crashes
        mid-propagation.

        If the client is already closed and the inbox doesn't have an in-progress
        APPLIED entry, we skip the handler entirely — no need to forward ABORT again.
        """
        try:
            _, client_id, sender_id, seq, _ = (
                self._internal_protocol.unpack_addressed_packet(message)
            )
            msg_id = f"abort:{sender_id}:{client_id}:{seq}"

            with self._lock:
                status = self._handler.inbox.classify(
                    client_id, sender_id, seq, MsgKind.ABORT
                )
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return

                abort_outputs = self._downstream_abort_outputs(client_id)
                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, b"",
                    lambda _: (FileIngestorState.abort_change(client_id), abort_outputs),
                    kind=MsgKind.ABORT,
                )

            if instruction.action is Action.PUBLISH_THEN_COMMIT:
                self._publish_outputs(instruction.outputs, self._downstream_senders())
                with self._lock:
                    self._handler.commit_done(*instruction.ctx)

            ack()
            logging.info(
                "file_ingestor_abort | id=%s | client_id=%s", self._config.id, client_id
            )
        except Exception as e:
            logging.error(
                "file_ingestor_abort_error | id=%s | error=%s", self._config.id, e, exc_info=True
            )
            nack(requeue=True)

    def _downstream_abort_outputs(self, client_id: int) -> list:
        """One ABORT packet per downstream shard so every worker cleans up."""
        abort_packet = self._packet(MessageType.ABORT, client_id, b"")
        return [
            (output.name, abort_packet, shard)
            for output in self._config.outputs
            for shard in range(output.shard_count)
        ]

    # ---------- control path ----------

    def _handle_control(self, message: bytes, ack, nack, response_senders: dict) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("file_ingestor_control_parse_error | id=%s", self._config.id)
            nack(requeue=False)
            return

        if msg_type == MessageType.FLUSH_ORDER:
            self._handle_flush_order(message, client_id, ctrl, ack, nack, response_senders)
            return

        # EOF_RECEIVED / PROCESSED_REQUEST: transient count-collection, not
        # WAL-tracked. A leader crash here is rebuilt by redelivery + coordinator retry.
        with self._lock:
            count = self._state.processed_count(client_id)
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
                nack(requeue=True)
        else:
            logging.warning(
                "file_ingestor_unexpected_control_action | id=%s | action=%s",
                self._config.id, action,
            )
            ack()

    def _handle_flush_order(self, message, client_id, ctrl, ack, nack, response_senders) -> None:
        """Non-leader handles FLUSH_ORDER: close (cleanup + drop) and send FLUSH_ACK
        with the forwarded count, all through the handler so a crash never
        double-flushes. The leader ignores its own FLUSH_ORDER."""
        leader_id = ctrl.sender_id
        msg_id = f"fo:{client_id}:{leader_id}"
        try:
            with self._lock:
                status = self._handler.inbox.classify(
                    client_id, leader_id, client_id, MsgKind.CTRL_FLUSH_ORDER
                )
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return
                if self._coordinator.leader_expected(client_id) is not None:
                    ack()  # leader for this client; ignores FLUSH_ORDER
                    return

                def bfn(_pl):
                    fwd = self._state.processed_count(client_id)
                    flush_ack_msg = self._coordinator.build_flush_ack(client_id, fwd)
                    flush_ack_dest = self._coordinator.response_queue_for(leader_id)
                    compound = FileIngestorState.compound_change(
                        FileIngestorState.coordinator_cleanup_change(client_id),
                        FileIngestorState.close_change(client_id),
                    )
                    return compound, [(flush_ack_dest, flush_ack_msg)]

                instruction = self._handler.handle(
                    msg_id, client_id, leader_id, client_id, message, bfn,
                    kind=MsgKind.CTRL_FLUSH_ORDER,
                )

            self._publish_outputs(instruction.outputs, response_senders)
            with self._lock:
                self._handler.commit_done(*instruction.ctx)
            ack()
        except Exception:
            logging.exception(
                "file_ingestor_flush_order_error | id=%s | client_id=%s",
                self._config.id, client_id,
            )
            nack(requeue=True)

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

    def _handle_response(
        self, message: bytes, ack, nack, control_senders: dict, eof_senders
    ) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("file_ingestor_response_parse_error | id=%s", self._config.id)
            nack(requeue=False)
            return

        if msg_type == MessageType.FLUSH_ACK:
            self._handle_flush_ack(message, client_id, ctrl, ack, nack, eof_senders)
            return

        # PROCESSED_ANSWER: transient count-collection on the leader. Direct
        # coordinator call; it accumulates and decides FLUSH_ORDER vs retry.
        with self._lock:
            action = self._coordinator.process_control_message(msg_type, client_id, ctrl)

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
                nack(requeue=True)
        else:
            logging.warning(
                "file_ingestor_unexpected_response_action | id=%s | action=%s",
                self._config.id, action,
            )
            ack()

    def _handle_flush_ack(self, message, client_id, ctrl, ack, nack, eof_senders) -> None:
        """Leader accumulates FLUSH_ACKs through the handler. business_fn predicts
        the last ack and emits the single downstream EOF (cross-replica total) to
        the outbox; the coordinator update and close happen in apply_change."""
        sender_id = ctrl.sender_id
        msg_id = f"fa:{client_id}:{sender_id}"
        try:
            with self._lock:
                status = self._handler.inbox.classify(
                    client_id, sender_id, client_id, MsgKind.CTRL_FLUSH_ACK
                )
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return
                if (
                    status is not InboxStatus.APPLIED
                    and self._coordinator.leader_expected(client_id) is None
                ):
                    ack()  # not the leader for this client; ignore
                    return

                already = self._coordinator.has_flush_ack(client_id, sender_id)
                new_ack_count = self._coordinator.flush_ack_count(client_id) + (
                    0 if already else 1
                )

                def bfn(_pl):
                    ack_change = FileIngestorState.coordinator_msg_change(
                        MessageType.FLUSH_ACK, client_id, sender_id,
                        ctrl.expected_total, ctrl.processed_count,
                    )
                    if new_ack_count >= self._config.total_instances - 1:
                        total_fwd = (
                            self._coordinator.forwarded_from_acks(client_id)
                            + (0 if already else ctrl.processed_count)
                            + self._state.processed_count(client_id)
                        )
                        outputs = self._downstream_eof_outputs(client_id, total_fwd)
                        compound = FileIngestorState.compound_change(
                            ack_change, FileIngestorState.close_change(client_id)
                        )
                        return compound, outputs
                    return ack_change, []

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, client_id, message, bfn,
                    kind=MsgKind.CTRL_FLUSH_ACK,
                )

            self._publish_outputs(instruction.outputs, eof_senders)
            with self._lock:
                self._handler.commit_done(*instruction.ctx)
            ack()
        except Exception:
            logging.exception(
                "file_ingestor_forward_eof_error | id=%s | client_id=%s",
                self._config.id, client_id,
            )
            nack(requeue=True)

    def _run_response_consumer(self) -> None:
        consumer = MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host, self._coordinator.my_response_queue()
        )
        control_senders = self._new_control_senders()
        eof_senders = self._new_downstream_senders()

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
                for sender in eof_senders.values():
                    sender.close()
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
                    msg, ack, nack, control_senders, eof_senders
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
                for sender in eof_senders.values():
                    sender.close()
            except Exception:
                pass
            try:
                consumer.close()
            except Exception:
                pass

    def start(self) -> None:
        self._ensure_output_bindings()
        logging.info(
            "file_ingestor_start | id=%s | mom_host=%s | exchange=%s | queue=%s | "
            "routing_key=%s | control_prefix=%s | response_prefix=%s | total_instances=%s",
            self._config.id,
            self._config.mom_host,
            self._config.input_exchange,
            self._config.queue_name,
            self._input_routing_key(),
            self._config.control_queue_prefix,
            self._config.response_queue_prefix,
            self._config.total_instances,
        )

        # Recover (restoring the coordinator) before starting the control/response
        # threads, so they don't touch coordinator state mid-recovery. The publisher
        # map covers filter exchanges + control + response queues because recovery
        # may re-publish a pending EOF to any of them.
        self._data_publishers = {
            **self._downstream_senders(),
            **self._main_control_senders,
            **self._new_response_senders(),
        }
        self._runner = WorkerRunner(
            handler=self._handler,
            publishers=self._data_publishers,
            process_payload=self._data_process_payload,
            lock=self._lock,
        )
        self._runner.recover_and_republish()

        self._control_thread = threading.Thread(target=self._run_control_consumer)
        self._response_thread = threading.Thread(target=self._run_response_consumer)
        self._control_thread.start()
        self._response_thread.start()

        self._input_queue = MessageMiddlewareExchangeRabbitMQ(
            self._config.mom_host,
            self._config.input_exchange,
            [self._input_routing_key()],
            queue_name=self._config.queue_name,
            exclusive=False,
        )
        try:
            if not self._stopped:
                self._input_queue.start_consuming(self._on_input_message)
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
            [self._input_queue]
            + list((self._downstream_outputs or {}).values())
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

    def _ensure_output_bindings(self) -> None:
        for output in self._config.outputs:
            def queue_name(index: int) -> str:
                return queue_name_for_worker(output.routing_prefix, index)

            ensure_exchange_queue_bindings(
                self._config.mom_host,
                output.exchange,
                {queue_name(index): queue_name(index) for index in range(output.shard_count)},
            )
