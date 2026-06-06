import logging
import threading
from typing import Callable, Optional

from common.message_protocol.internal import InternalProtocol
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.middleware import MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ


class EofCoordinator:
    """
    Coordina el protocolo EOF entre réplicas de un worker.

    mode="broadcast":
        Una sola réplica recibe el EOF de upstream. Esa réplica se convierte en
        líder dinámico: transmite EOF_RECEIVED a todas las réplicas vía el
        control_exchange. Cada réplica toma un snapshot de su conteo y reporta
        PROCESSED_ANSWER a la cola de respuesta del líder. Cuando el total
        acumulado >= expected_total, el líder llama on_flush en su propio hilo.

    mode="flush_order":
        Cada réplica tiene su propio shard de entrada y recibe su EOF
        independientemente. Cada una reporta directamente a la cola de respuesta
        del líder fijo (leader_id). Cuando el líder acumula suficiente, difunde
        FLUSH_ORDER por el control_exchange y on_flush se llama en cada réplica.

    En ambos modos, si total_instances == 1, se omite toda la coordinación y
    on_flush se llama directamente en handle_upstream_eof.

    Firma de on_flush: (client_id: int, total_processed: int, total_forwarded: int)
      - mode="broadcast": totals son los acumulados del líder (útiles para reenviar EOF).
      - mode="flush_order": totals son 0; cada réplica emite su propio shard.
    """

    def __init__(
        self,
        instance_id: int,
        total_instances: int,
        mom_host: str,
        control_exchange: str,
        response_queue_prefix: str,
        on_flush: Callable[[int, int, int], None],
        get_count: Callable[[int], int],
        mode: str = "broadcast",
        get_forwarded: Optional[Callable[[int], int]] = None,
        leader_id: int = 0,
    ):
        if mode not in ("broadcast", "flush_order"):
            raise ValueError(f"EofCoordinator mode must be 'broadcast' or 'flush_order', got {mode!r}")

        self._id = instance_id
        self._total = total_instances
        self._mom_host = mom_host
        self._control_exchange = control_exchange
        self._response_queue_prefix = response_queue_prefix
        self._on_flush = on_flush
        self._get_count = get_count
        self._get_forwarded = get_forwarded
        self._mode = mode
        self._leader_id = leader_id
        self._is_leader = (instance_id == leader_id)

        self._lock = threading.Lock()
        self._stopped = threading.Event()

        self._internal_protocol = InternalProtocol()
        self._control_serializer = ControlMessageSerializer()

        # Estado por cliente — broadcast mode
        # client_id → (expected_total, leader_id)
        self._pending_eof: dict[int, tuple[int, int]] = {}
        self._leader_expected: dict[int, int] = {}   # líder dinámico: fijado en handle_upstream_eof
        self._leader_processed: dict[int, int] = {}
        self._leader_forwarded: dict[int, int] = {}

        # Estado por cliente — flush_order mode
        self._seen_eof: set[int] = set()             # réplicas que ya vieron su EOF de shard
        self._shard_expected: dict[int, int] = {}    # líder fijo: expected total
        self._flushed: set[int] = set()              # evita doble FLUSH_ORDER

        self._control_consumer: Optional[MessageMiddlewareExchangeRabbitMQ] = None
        self._response_consumer: Optional[MessageMiddlewareQueueRabbitMQ] = None
        self._control_thread: Optional[threading.Thread] = None
        self._response_thread: Optional[threading.Thread] = None

    # ─── helpers ─────────────────────────────────────────────────────────────

    def _response_queue_name(self, for_id: int) -> str:
        return f"{self._response_queue_prefix}_{for_id}"

    def _make_packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _make_answer_payload(
        self, sender_id: int, processed_count: int, forwarded_count: int
    ) -> bytes:
        # PROCESSED_ANSWER: processed_count → campo processed_count;
        #                   forwarded_count → campo expected_total (transporte).
        return self._control_serializer.serialize(
            ControlMessage(
                sender_id=sender_id,
                expected_total=forwarded_count,
                processed_count=processed_count,
            )
        )

    def _make_announcement_payload(self, sender_id: int, expected_total: int) -> bytes:
        # EOF_RECEIVED / FLUSH_ORDER: expected_total → campo expected_total (igual que el EOF de upstream).
        return self._control_serializer.serialize(
            ControlMessage(
                sender_id=sender_id,
                expected_total=expected_total,
                processed_count=0,
            )
        )

    def _send_to_leader(
        self,
        client_id: int,
        leader_id: int,
        processed: int,
        forwarded: int,
    ) -> None:
        q = MessageMiddlewareQueueRabbitMQ(
            self._mom_host, self._response_queue_name(leader_id)
        )
        try:
            q.send(
                self._make_packet(
                    MessageType.PROCESSED_ANSWER,
                    client_id,
                    self._make_answer_payload(self._id, processed, forwarded),
                )
            )
        finally:
            q.close()

    def _broadcast_msg(self, msg_type: MessageType, client_id: int, payload: bytes) -> None:
        sender = MessageMiddlewareExchangeRabbitMQ(
            self._mom_host, self._control_exchange, [self._control_exchange]
        )
        try:
            sender.send(self._make_packet(msg_type, client_id, payload))
        finally:
            sender.close()

    # ─── API pública ──────────────────────────────────────────────────────────

    def handle_upstream_eof(self, client_id: int, expected_total: int) -> None:
        """
        Llamar cuando llega un EOF de upstream para client_id.
        En modo broadcast: esta réplica se vuelve líder y difunde EOF_RECEIVED.
        En modo flush_order: cada réplica reporta directamente al líder fijo.
        Si total_instances == 1 se llama on_flush directamente.
        """
        if self._total <= 1:
            fwd = self._get_forwarded(client_id) if self._get_forwarded else 0
            self._on_flush(client_id, expected_total, fwd)
            return

        if self._mode == "broadcast":
            with self._lock:
                self._leader_expected[client_id] = expected_total
            logging.info(
                "eof_coordinator_broadcast | id=%s | client_id=%s | expected_total=%s",
                self._id, client_id, expected_total,
            )
            self._broadcast_msg(
                MessageType.EOF_RECEIVED,
                client_id,
                self._make_announcement_payload(self._id, expected_total),
            )
        else:  # flush_order
            count = self._get_count(client_id)
            fwd = self._get_forwarded(client_id) if self._get_forwarded else 0
            with self._lock:
                if client_id in self._seen_eof:
                    return  # EOF duplicado para este shard
                self._seen_eof.add(client_id)
                if self._is_leader:
                    self._shard_expected[client_id] = expected_total
            logging.info(
                "eof_coordinator_report | id=%s | client_id=%s | count=%s | "
                "expected_total=%s",
                self._id, client_id, count, expected_total,
            )
            self._send_to_leader(client_id, self._leader_id, count, fwd)

    def report_late_data(
        self, client_id: int, delta_processed: int, delta_forwarded: int
    ) -> None:
        """
        Llamar cuando llega DATA después de haber visto el EOF (deltas tardíos).
        Solo relevante en modo broadcast con workers que siguen procesando después
        de recibirlo (ej. sum, filter_q5_usd).
        """
        with self._lock:
            pending = self._pending_eof.get(client_id)
        if pending is None:
            return
        _, leader_id = pending
        self._send_to_leader(client_id, leader_id, delta_processed, delta_forwarded)

    def start(self) -> None:
        """Lanza los threads de coordinación. Llamar antes de iniciar el consumo de datos."""
        if self._total <= 1:
            return

        self._control_thread = threading.Thread(
            target=self._run_control_consumer, daemon=True
        )
        self._control_thread.start()

        # En broadcast: cualquier réplica puede ser líder → todas necesitan consumer de response.
        # En flush_order: solo el líder fijo (leader_id) necesita consumer de response.
        if self._mode == "broadcast" or self._is_leader:
            self._response_thread = threading.Thread(
                target=self._run_response_consumer, daemon=True
            )
            self._response_thread.start()

    def stop(self) -> None:
        """Señala el apagado y detiene los consumers."""
        self._stopped.set()
        if self._control_consumer is not None:
            self._control_consumer.request_stop_consuming()
        if self._response_consumer is not None:
            self._response_consumer.request_stop_consuming()

    def join(self, timeout: float = 5.0) -> None:
        if self._control_thread is not None:
            self._control_thread.join(timeout=timeout)
        if self._response_thread is not None:
            self._response_thread.join(timeout=timeout)

    # ─── consumer: control exchange ──────────────────────────────────────────

    def _run_control_consumer(self) -> None:
        consumer = MessageMiddlewareExchangeRabbitMQ(
            self._mom_host, self._control_exchange, [self._control_exchange]
        )
        self._control_consumer = consumer
        handler = (
            self._on_eof_received_broadcast
            if self._mode == "broadcast"
            else self._on_flush_order
        )
        try:
            if not self._stopped.is_set():
                consumer.start_consuming(handler)
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    def _on_eof_received_broadcast(self, message: bytes, ack, nack) -> None:
        """Control consumer — modo broadcast: recibe EOF_RECEIVED, snapshot y reporta."""
        try:
            msg_type, client_id, payload = self._internal_protocol.unpack_packet(message)
            if msg_type != MessageType.EOF_RECEIVED:
                raise ValueError(f"eof_coordinator: unexpected control msg type {msg_type}")

            ctrl = self._control_serializer.deserialize(payload)
            leader_id = ctrl.sender_id
            expected_total = ctrl.expected_total

            with self._lock:
                if client_id in self._pending_eof:
                    ack()
                    return  # broadcast duplicado
                self._pending_eof[client_id] = (expected_total, leader_id)
                count = self._get_count(client_id)
                fwd = self._get_forwarded(client_id) if self._get_forwarded else 0

            logging.info(
                "eof_coordinator_snapshot | id=%s | client_id=%s | leader_id=%s | "
                "count=%s | fwd=%s",
                self._id, client_id, leader_id, count, fwd,
            )
            self._send_to_leader(client_id, leader_id, count, fwd)
            ack()
        except Exception:
            logging.exception("eof_coordinator_control_error | id=%s", self._id)
            nack()

    def _on_flush_order(self, message: bytes, ack, nack) -> None:
        """Control consumer — modo flush_order: recibe FLUSH_ORDER, llama on_flush."""
        try:
            msg_type, client_id, _ = self._internal_protocol.unpack_packet(message)
            if msg_type != MessageType.FLUSH_ORDER:
                raise ValueError(f"eof_coordinator: unexpected flush msg type {msg_type}")

            logging.info(
                "eof_coordinator_flush_order | id=%s | client_id=%s", self._id, client_id
            )
            self._on_flush(client_id, 0, 0)
            ack()
        except Exception:
            logging.exception("eof_coordinator_flush_order_error | id=%s", self._id)
            nack()

    # ─── consumer: response queue (líder) ────────────────────────────────────

    def _run_response_consumer(self) -> None:
        consumer = MessageMiddlewareQueueRabbitMQ(
            self._mom_host, self._response_queue_name(self._id)
        )
        self._response_consumer = consumer
        try:
            if not self._stopped.is_set():
                consumer.start_consuming(self._on_processed_answer)
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    def _on_processed_answer(self, message: bytes, ack, nack) -> None:
        """Response consumer: acumula conteos y dispara flush cuando corresponde."""
        try:
            msg_type, client_id, payload = self._internal_protocol.unpack_packet(message)
            if msg_type != MessageType.PROCESSED_ANSWER:
                raise ValueError(f"eof_coordinator: unexpected response msg type {msg_type}")

            ctrl = self._control_serializer.deserialize(payload)

            if self._mode == "broadcast":
                flush_args = self._accumulate_broadcast(client_id, ctrl)
                if flush_args is not None:
                    total_p, total_f = flush_args
                    logging.info(
                        "eof_coordinator_done_broadcast | id=%s | client_id=%s | "
                        "total_processed=%s | total_forwarded=%s",
                        self._id, client_id, total_p, total_f,
                    )
                    self._on_flush(client_id, total_p, total_f)
            else:
                should_broadcast = self._accumulate_flush_order(client_id, ctrl)
                if should_broadcast:
                    logging.info(
                        "eof_coordinator_send_flush_order | id=%s | client_id=%s",
                        self._id, client_id,
                    )
                    self._broadcast_msg(
                        MessageType.FLUSH_ORDER,
                        client_id,
                        self._make_announcement_payload(self._id, 0),
                    )
            ack()
        except Exception:
            logging.exception("eof_coordinator_response_error | id=%s", self._id)
            nack()

    def _accumulate_broadcast(
        self, client_id: int, ctrl: ControlMessage
    ) -> Optional[tuple[int, int]]:
        """
        Acumula el PROCESSED_ANSWER en el líder dinámico.
        Retorna (total_processed, total_forwarded) si se alcanzó el umbral, o None.
        """
        with self._lock:
            expected = self._leader_expected.get(client_id)
            if expected is None:
                # Esta réplica no es el líder para este cliente; ignorar.
                return None

            self._leader_processed[client_id] = (
                self._leader_processed.get(client_id, 0) + ctrl.processed_count
            )
            self._leader_forwarded[client_id] = (
                self._leader_forwarded.get(client_id, 0) + ctrl.expected_total
            )

            if self._leader_processed[client_id] < expected:
                return None

            # Umbral alcanzado: limpiar y retornar totales.
            total_p = self._leader_processed.pop(client_id)
            total_f = self._leader_forwarded.pop(client_id, 0)
            self._leader_expected.pop(client_id)
            self._pending_eof.pop(client_id, None)
            return total_p, total_f

    def _accumulate_flush_order(self, client_id: int, ctrl: ControlMessage) -> bool:
        """
        Acumula el PROCESSED_ANSWER en el líder fijo.
        Retorna True si debe difundir FLUSH_ORDER.
        """
        with self._lock:
            if client_id in self._flushed:
                return False

            self._leader_processed[client_id] = (
                self._leader_processed.get(client_id, 0) + ctrl.processed_count
            )
            expected = self._shard_expected.get(client_id)

            if expected is None or self._leader_processed[client_id] < expected:
                return False

            self._flushed.add(client_id)
            self._leader_processed.pop(client_id, None)
            self._shard_expected.pop(client_id, None)
            return True
