import logging
import os
import threading

from common import middleware
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import PersistentStateHandler
from common.fault_tolerance.inbox import MsgKind
from common.message_protocol.internal import (
    ControlMessage,
    ControlMessageSerializer,
    InternalProtocol,
    Q3AverageResultSerializer,
    TransactionSerializer,
)
from common.message_protocol.internal.common import MessageType
from common.routing import queue_name_for_worker

try:
    from q3_barrier_state import Q3BarrierState
except ImportError:
    from workers.q3_barrier.q3_barrier_state import Q3BarrierState


ID = int(os.environ.get("ID", "0"))
MOM_HOST = os.environ["MOM_HOST"]
AVERAGES_QUEUE = os.environ["Q3_AVERAGES_QUEUE"]
CANDIDATES_QUEUE = os.environ["Q3_CANDIDATES_QUEUE"]
OUTPUT_QUEUE = os.environ["GATEWAY_Q3_QUEUE"]
THRESHOLD_DIVISOR = float(os.getenv("Q3_THRESHOLD_DIVISOR", "100"))

Q3_BARRIER_AMOUNT = int(os.getenv("Q3_BARRIER_AMOUNT", "1"))
Q3_AVERAGES_EXCHANGE = os.environ["Q3_AVERAGES_EXCHANGE"]
Q3_CANDIDATES_EXCHANGE = os.environ["Q3_CANDIDATES_EXCHANGE"]
Q3_AVERAGES_ROUTING_PREFIX = os.environ["Q3_AVERAGES_ROUTING_PREFIX"]
Q3_CANDIDATES_ROUTING_PREFIX = os.environ["Q3_CANDIDATES_ROUTING_PREFIX"]
Q3_OUTPUT_BATCH_BYTES = int(os.getenv("Q3_OUTPUT_BATCH_BYTES", str(1024 * 1024)))
Q3_OUTPUT_BATCH_MAX_TX = int(os.getenv("Q3_OUTPUT_BATCH_MAX_TX", "5000"))
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/q3_barrier_state")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))


class Q3BarrierWorker:
    """Joins two per-client streams (averages + candidates) behind a barrier and,
    once both signal EOF, streams the candidate disk log and emits the ones below
    average/THRESHOLD_DIVISOR. Durable via PersistentStateHandler: averages and the
    candidate disk log are WAL-tracked; a crash replays exactly.

    Both input streams are sharded by client_id to per-replica queues, so a client's
    averages and candidates co-locate on one replica and redelivery returns to it —
    the per-replica inbox dedup is valid. The two streams share one handler/inbox and
    are separated by MsgKind so their (sender, seq) can never collide.
    """

    def __init__(self):
        self.averages_input = self._build_input(
            Q3_AVERAGES_EXCHANGE, Q3_AVERAGES_ROUTING_PREFIX
        )
        self.candidates_input = self._build_input(
            Q3_CANDIDATES_EXCHANGE, Q3_CANDIDATES_ROUTING_PREFIX
        )

        self.internal_protocol = InternalProtocol()
        self.control_serializer = ControlMessageSerializer()
        self.transaction_serializer = TransactionSerializer()

        self._state = Q3BarrierState(STATE_DIR, node_id=f"q3b_{ID}")
        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"q3b_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
        )

        self._lock = threading.Lock()
        self.candidates_thread: threading.Thread | None = None
        self.closed = False
        self._stopped = False

    def _build_input(self, exchange_name: str, routing_prefix: str):
        return middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            exchange_name,
            routing_keys=[queue_name_for_worker(routing_prefix, ID)],
            queue_name=queue_name_for_worker(routing_prefix, ID),
            exclusive=False,
        )

    def _new_output_sender(self):
        return middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    # ---------- helpers ----------

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        # OUTPUT_QUEUE is the gateway (not WAL-wired) — basic packets.
        return self.internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _publish_commit_ack(self, instruction, ack, output_sender) -> bool:
        """Publish outputs (all to OUTPUT_QUEUE), commit, ack. A duplicate the inbox
        already finished returns Action.ACK with ctx=None — just ack it."""
        if instruction.action is Action.ACK:
            ack()
            return False
        for entry in instruction.outputs:
            output_sender.send(entry.body)
        with self._lock:
            self._handler.commit_done(*instruction.ctx)
        ack()
        return True

    def _emit_outputs(self, client_id: int) -> list:
        """Logical outputs for the barrier emit: stream the candidate disk log,
        keep the transactions below average/THRESHOLD_DIVISOR, batch them, and append
        a final EOF. O(1) RAM (disk log is read via a generator); the result is the
        small filtered tail, so the outbox stays bounded. Read at NEW time only —
        replay takes these from the WAL."""
        averages = self._state.averages(client_id)
        disk_log = self._state.disk_log(client_id)
        outputs = []
        batch = []
        batch_bytes = 0
        emitted = 0

        if disk_log is not None:
            for raw_batch in disk_log.iter_raw_batches():
                for tx in self.transaction_serializer.deserialize_batch(raw_batch):
                    avg = averages.get(tx.format)
                    if avg is None or tx.amount >= (avg / THRESHOLD_DIVISOR):
                        continue
                    batch.append(tx)
                    batch_bytes += TransactionSerializer.SIZE
                    emitted += 1
                    if len(batch) >= Q3_OUTPUT_BATCH_MAX_TX or batch_bytes >= Q3_OUTPUT_BATCH_BYTES:
                        outputs.append((OUTPUT_QUEUE, self._packet(
                            MessageType.DATA, client_id,
                            self.transaction_serializer.serialize_batch(batch),
                        )))
                        batch = []
                        batch_bytes = 0
            if batch:
                outputs.append((OUTPUT_QUEUE, self._packet(
                    MessageType.DATA, client_id,
                    self.transaction_serializer.serialize_batch(batch),
                )))

        eof_payload = self.control_serializer.serialize(
            ControlMessage(sender_id=ID, expected_total=emitted, processed_count=0)
        )
        outputs.append((OUTPUT_QUEUE, self._packet(MessageType.EOF, client_id, eof_payload)))
        logging.info(
            "q3_barrier_emit | id=%s | client_id=%s | averages=%s | results=%s",
            ID, client_id, len(averages), emitted,
        )
        return outputs

    # ---------- averages stream ----------

    def _handle_average(self, message, ack, nack, output_sender):
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self.internal_protocol.unpack_addressed_packet(message)
            )
            msg_id = f"avg:{sender_id}:{client_id}:{seq}"
            with self._lock:
                if self._state.is_closed(client_id):
                    ack()
                    return
                if msg_type == MessageType.DATA:
                    result = Q3AverageResultSerializer.deserialize(payload)

                    def bfn(_pl):
                        return Q3BarrierState.avg_data_change(
                            client_id, result.payment_format, result.average
                        ), []
                elif msg_type == MessageType.EOF:
                    ready = self._state.has_candidate_eof(client_id)
                    logging.info(
                        "q3_barrier_avg_eof | id=%s | client_id=%s | ready=%s",
                        ID, client_id, ready,
                    )

                    def bfn(_pl):
                        eof_change = Q3BarrierState.avg_eof_change(client_id)
                        if ready:  # candidates already done -> barrier closes here
                            outputs = self._emit_outputs(client_id)
                            return Q3BarrierState.compound_change(
                                eof_change, Q3BarrierState.close_change(client_id)
                            ), outputs
                        return eof_change, []
                else:
                    raise ValueError(f"unsupported Q3 averages message type: {msg_type}")

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, payload, bfn,
                    kind=MsgKind.Q3_AVERAGES,
                )
            self._publish_commit_ack(instruction, ack, output_sender)
        except Exception:
            logging.exception("q3_barrier_average_error | id=%s", ID)
            nack(requeue=True)

    # ---------- candidates stream ----------

    def _handle_candidate(self, message, ack, nack, output_sender):
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self.internal_protocol.unpack_addressed_packet(message)
            )
            msg_id = f"cand:{sender_id}:{client_id}:{seq}"
            with self._lock:
                if self._state.is_closed(client_id):
                    ack()
                    return
                if msg_type == MessageType.DATA:
                    def bfn(pl):
                        return Q3BarrierState.candidate_data_change(client_id, pl), []
                elif msg_type == MessageType.EOF:
                    control = self.control_serializer.deserialize(payload)
                    expected_total = control.expected_total
                    ready = self._state.has_avg_eof(client_id)
                    logging.info(
                        "q3_barrier_candidate_eof | id=%s | client_id=%s | "
                        "ready=%s | expected_total=%s",
                        ID, client_id, ready, expected_total,
                    )

                    def bfn(_pl):
                        eof_change = Q3BarrierState.candidate_eof_change(
                            client_id, expected_total
                        )
                        if ready:  # averages already done -> barrier closes here
                            outputs = self._emit_outputs(client_id)
                            return Q3BarrierState.compound_change(
                                eof_change, Q3BarrierState.close_change(client_id)
                            ), outputs
                        return eof_change, []
                else:
                    raise ValueError(f"unsupported Q3 candidates message type: {msg_type}")

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, payload, bfn,
                    kind=MsgKind.Q3_CANDIDATES,
                )
            self._publish_commit_ack(instruction, ack, output_sender)
        except Exception:
            logging.exception("q3_barrier_candidate_error | id=%s", ID)
            nack(requeue=True)

    def _consume_candidates(self):
        output_sender = self._new_output_sender()
        try:
            if not self._stopped:
                self.candidates_input.start_consuming(
                    lambda message, ack, nack: self._handle_candidate(
                        message, ack, nack, output_sender
                    )
                )
        finally:
            for resource in (self.candidates_input, output_sender):
                try:
                    resource.close()
                except Exception:
                    pass

    # ---------- lifecycle ----------

    def start(self):
        logging.info(
            "q3_barrier_start | id=%s | averages_queue=%s | candidates_queue=%s | "
            "output_queue=%s",
            ID, AVERAGES_QUEUE, CANDIDATES_QUEUE, OUTPUT_QUEUE,
        )

        # Recover (rebuilds averages + the candidate disk log from snapshot + WAL)
        # and re-publish any pending outbox BEFORE consuming, so the streams never
        # touch shared state mid-recovery.
        recovery_sender = self._new_output_sender()
        with self._lock:
            self._handler.recover()
            pending = self._handler.outbox_to_republish()
        for entry in pending:
            recovery_sender.send(entry.body)
        try:
            recovery_sender.close()
        except Exception:
            pass

        self.candidates_thread = threading.Thread(target=self._consume_candidates)
        self.candidates_thread.start()

        averages_sender = self._new_output_sender()
        try:
            if not self._stopped:
                self.averages_input.start_consuming(
                    lambda message, ack, nack: self._handle_average(
                        message, ack, nack, averages_sender
                    )
                )
        finally:
            self.handle_sigterm()
            if self.candidates_thread is not None:
                self.candidates_thread.join(timeout=5)
            try:
                averages_sender.close()
            except Exception:
                pass
            self.close()

    def handle_sigterm(self):
        if self._stopped:
            return
        self._stopped = True
        logging.info("q3_barrier_shutdown | id=%s", ID)
        self.averages_input.request_stop_consuming()
        self.candidates_input.request_stop_consuming()

    def close(self):
        if self.closed:
            return
        self.closed = True
        # candidates_input is closed by its own thread.
        try:
            self.averages_input.close()
        except Exception as e:
            logging.warning("q3_barrier_close_error | id=%s | error=%s", ID, e)
