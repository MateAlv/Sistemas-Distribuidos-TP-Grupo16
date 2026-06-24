import logging
from dataclasses import dataclass
from typing import Optional, Union

from common.message_protocol.internal import InternalProtocol
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer


# ─── Acciones retornadas por el coordinador ──────────────────────────────────

@dataclass
class SendAnswerAction:
    """
    Orden dada a un worker no lider para enviar un mensaje de control a la response_queue del líder.
    El tipo de mensaje es PROCESSED_ANSWER

    En PROCESSED_ANSWER el worker envía su conteo actual de mensajes procesados (que llegaron)
    y forwardeados (que envió).
    """
    queue_name: str
    message: bytes


@dataclass
class BroadcastAction:
    """
    Orden dada a un worker lider de eviar un mensaje a todas las control_queues.

    Los mensajes posibles son:
    - EOF_RECEIVED: el líder dinámico de broadcast notifica a todos los workers que recibió EOF upstream.
    - PROCESSED_REQUEST: el líder solicita a todos los workers que reenvíen su conteo actual 
    (snapshot) porque el conteo acumulado no alcanzó el esperado.
    - FLUSH_ORDER: el líder notifica a todos los workers que pueden hacer flush y enviar FLUSH_ACK.

    sleep_before > 0: el worker debe dormir ese tiempo ANTES de enviar.
    Usado en retries para que el thread de datos pueda procesar mensajes pendientes
    antes de que el líder solicite nuevos conteos.
    Esto solo ocurre en PROCESSED_REQUEST, cuando el conteo acumulado no alcanza el esperado.
    """
    queue_names: list  # list[str]
    message: bytes
    sleep_before: float = 0.0


@dataclass
class FlushAction:
    """
    Orden dada a un worker para ejecutar la lógica de flush del cliente.

    is_leader=True : líder — emitir EOF downstream con total_forwarded.
    is_leader=False: no-líder — hacer flush local y luego enviar FLUSH_ACK a ack_queue.
    El worker construye el FLUSH_ACK con coordinator.build_flush_ack(client_id, forwarded).
    """
    is_leader: bool
    total_forwarded: int = 0   # solo líder: suma de forwardeados de todos los workers
    ack_queue: str = ""        # solo no-líder: response_queue del líder


CoordinatorAction = Union[SendAnswerAction, BroadcastAction, FlushAction]


class EofCoordinator:
    """
    Máquina de estados para el protocolo EOF entre réplicas de un worker.

    El worker es responsable de:
      · Adquirir su propio lock antes de llamar cualquier método de estado
      · Ejecutar la acción retornada (I/O) FUERA del lock usado para procesar el mensaje de control
      · Si un SendAnswerAction falla: llamar clear_pending_eof() bajo lock
        y hacer nack(requeue=True) para que el mensaje sea reentregado

    Dos modos de coordinación: "broadcast" y "flush_order".  El modo se fija al crear el coordinador
    por variable de entorno.

    # MODO "broadcast"  —  cola de entrada compartida entre réplicas

    Este modo es usado por workers que consumen de la misma cola de entrada.
    Una réplica recibe el EOF de upstream (la que desencola primero).
    Esa réplica se convierte en LÍDER DINÁMICO para ese client_id.

      Paso 1 · (data thread, bajo lock)  worker recibe EOF upstream
        action = coordinator.on_upstream_eof(client_id, expected_total, count, fwd)
        retorna → BroadcastAction(EOF_RECEIVED) a todas las control_queues (todos los otros workers)
        si N==1 → FlushAction(is_leader=True, total_forwarded=fwd) (worker unico, no hay coordinacion)

      Paso 2 · (control thread, bajo lock)  todas las réplicas reciben EOF_RECEIVED
        action = coordinator.process_control_message(EOF_RECEIVED, client_id, ctrl, count, fwd)
        retorna → SendAnswerAction(PROCESSED_ANSWER) a response_queue del líder (devolver procesados al lider)
        retorna → None si es EOF_RECEIVED duplicado para este client_id
        Si el envío falla: coordinator.clear_pending_eof(client_id) + nack(requeue=True)

      Paso 3 · (response thread, bajo lock)  líder acumula PROCESSED_ANSWERs
        action = coordinator.process_control_message(PROCESSED_ANSWER, client_id, ctrl)
        retorna → None mientras no hayan respondido todos los workers (N total)
        Cuando llegan todos los PROCESSED_ANSWERs (N):
        retorna → BroadcastAction(FLUSH_ORDER)        si total >= expected
        retorna → BroadcastAction(PROCESSED_REQUEST,  si total < expected (retry)
                    sleep_before=0.5)                  el worker duerme, luego re-broadcast
                    
      Paso 3b · (control thread, bajo lock)  workers reciben PROCESSED_REQUEST (retry)
        action = coordinator.process_control_message(PROCESSED_REQUEST, client_id, ctrl, count, fwd)
        retorna → SendAnswerAction(PROCESSED_ANSWER) con el conteo actual (snapshot fresco)
        Esto solo ocurre si el conteo acumulado no alcanzó el esperado. El líder solicita reenvío de conteos.

      Paso 4 · (control thread, bajo lock)  réplicas reciben FLUSH_ORDER
        action = coordinator.process_control_message(FLUSH_ORDER, client_id, ctrl)
        líder    → None (warning, lo ignora — ya coordinó este client_id)
        no-líder → FlushAction(is_leader=False, ack_queue=response_queue_del_líder)
                   El worker hace su flush, construye FLUSH_ACK con
                   coordinator.build_flush_ack(client_id, forwarded) y lo envía a ack_queue,
                   luego llama coordinator.cleanup_client(client_id) bajo lock

      Paso 5 · (response thread, bajo lock)  líder acumula FLUSH_ACKs
        action = coordinator.process_control_message(FLUSH_ACK, client_id, ctrl)
        retorna → None mientras no hayan enviado FLUSH_ACK todos los no-líderes (N-1)
        Cuando llegan todos los FLUSH_ACKs (N-1):
        retorna → FlushAction(is_leader=True, total_forwarded=total) cuando llegan todos
                  El líder hace su flush y emite EOF downstream con total_forwarded

    # MODO "flush_order"  —  cada réplica tiene su propio shard de entrada

    Cada réplica recibe el EOF de su shard de forma independiente.
    El líder es siempre el worker con instance_id == leader_id (por defecto 0).
    No hay paso de broadcast inicial; cada réplica reporta directamente al líder.

      Paso 1 · (data thread, bajo lock)  cada réplica recibe su EOF de shard
        action = coordinator.on_upstream_eof(client_id, expected_total, count, fwd)
        retorna → SendAnswerAction(PROCESSED_ANSWER) a response_queue del líder fijo
        si N==1 → FlushAction(is_leader=True, total_forwarded=0)

      Paso 2 · (response thread, bajo lock)  líder acumula PROCESSED_ANSWERs
        action = coordinator.process_control_message(PROCESSED_ANSWER, client_id, ctrl)
        retorna → None mientras no hayan respondido todos los workers (N total)
        retorna → BroadcastAction(FLUSH_ORDER)       si total >= expected
        retorna → BroadcastAction(PROCESSED_REQUEST, si total < expected (retry)
                    sleep_before=0.5)

      Paso 2b · (control thread, bajo lock)  workers reciben PROCESSED_REQUEST (retry)
        action = coordinator.process_control_message(PROCESSED_REQUEST, client_id, ctrl, count, fwd)
        retorna → SendAnswerAction(PROCESSED_ANSWER) con conteo actualizado

      Paso 3 · (control thread, bajo lock)  réplicas reciben FLUSH_ORDER
        action = coordinator.process_control_message(FLUSH_ORDER, client_id, ctrl)
        líder    → None (lo ignora)
        no-líder → FlushAction(is_leader=False, ack_queue=response_queue_del_líder)
                   El worker hace flush, envía FLUSH_ACK y llama cleanup_client()

      Paso 4 · (response thread, bajo lock)  líder acumula FLUSH_ACKs
        action = coordinator.process_control_message(FLUSH_ACK, client_id, ctrl)
        retorna → None mientras no hayan enviado FLUSH_ACK todos los no-líderes (N-1)
        retorna → FlushAction(is_leader=True, total_forwarded=total) cuando llegan todos
    """

    def __init__(
        self,
        instance_id: int,
        total_instances: int,
        control_queue_prefix: str,
        response_queue_prefix: str,
        mode: str = "broadcast",
        leader_id: int = 0,
    ):
        if mode not in ("broadcast", "flush_order"):
            raise ValueError(f"mode must be 'broadcast' or 'flush_order', got {mode!r}")

        self._id = instance_id
        self._total = total_instances
        self._control_prefix = control_queue_prefix
        self._response_prefix = response_queue_prefix
        self._mode = mode
        self._leader_id = leader_id
        self._is_leader = (instance_id == leader_id)

        self._proto = InternalProtocol()
        self._ctrl_ser = ControlMessageSerializer()

        # Estado: no-líder broadcast — set cuando se procesa EOF_RECEIVED
        # client_id → (expected_total, leader_id)
        self._pending_eof: dict[int, tuple[int, int]] = {}

        # Estado: flush_order — evita doble reporte por EOF duplicado
        self._seen_eof: set[int] = set()

        # Estado: líder (ambos modos)
        # expected_total del EOF upstream (broadcast: dinámico; flush_order: líder fijo)
        self._leader_expected: dict[int, int] = {}
        # accumulated processed_count de PROCESSED_ANSWERs
        self._leader_processed: dict[int, int] = {}
        # set de sender_ids que ya enviaron PROCESSED_ANSWER en la ronda actual
        self._leader_responders: dict[int, set] = {}
        # set de sender_ids que ya enviaron FLUSH_ACK
        self._flush_acks: dict[int, set] = {}
        # accumulated forwarded reportado en cada FLUSH_ACK
        self._forwarded_from_acks: dict[int, int] = {}

    # ─── nombres de colas ────────────────────────────────────────────────────

    def control_queue_for(self, instance_id: int) -> str:
        return f"{self._control_prefix}_{instance_id}"

    def response_queue_for(self, instance_id: int) -> str:
        return f"{self._response_prefix}_{instance_id}"

    def my_control_queue(self) -> str:
        return self.control_queue_for(self._id)

    def my_response_queue(self) -> str:
        return self.response_queue_for(self._id)

    def needs_response_consumer(self) -> bool:
        """True si este worker debe escuchar en su response_queue.

        Broadcast: cualquier réplica puede ser líder dinámico → todas escuchan.
        Flush_order: solo el líder fijo acumula PROCESSED_ANSWERs y FLUSH_ACKs.
        """
        return self._mode == "broadcast" or self._is_leader

    # ─── construcción de mensajes ─────────────────────────────────────────────

    def build_answer(self, client_id: int, processed: int, forwarded: int) -> bytes:
        """Construir PROCESSED_ANSWER listo para enviar."""
        return self._make_packet(
            MessageType.PROCESSED_ANSWER, client_id, self._id,
            expected_total=forwarded,
            processed_count=processed,
        )

    def build_flush_ack(self, client_id: int, forwarded: int) -> bytes:
        """Construir FLUSH_ACK listo para enviar al líder.

        forwarded: mensajes enviados a downstream por este worker para client_id.
        """
        return self._make_packet(
            MessageType.FLUSH_ACK, client_id, self._id,
            expected_total=0,
            processed_count=forwarded,
        )

    def parse_message(self, raw: bytes) -> tuple:
        """Desempaquetar bytes → (msg_type, client_id, ControlMessage).
        Llamar fuera del lock (parsing puro, sin acceso a estado).
        """
        msg_type, client_id, payload = self._proto.unpack_packet(raw)
        ctrl = self._ctrl_ser.deserialize(payload)
        return msg_type, client_id, ctrl

    # ─── cleanup de estado ───────────────────────────────────────────────────

    def clear_pending_eof(self, client_id: int) -> None:
        """Limpiar pending_eof para permitir reintento del paso 2 (broadcast mode).
        Llamar bajo lock si el SendAnswerAction falló, antes de nack(requeue=True).
        """
        self._pending_eof.pop(client_id, None)

    def cleanup_client(self, client_id: int) -> None:
        """Limpiar estado del cliente tras un flush de no-líder.
        Llamar bajo lock después de que el worker procesó un FlushAction(is_leader=False).
        El líder limpia su estado automáticamente en _on_flush_ack.
        """
        self._pending_eof.pop(client_id, None)
        self._seen_eof.discard(client_id)

    def cleanup_leader_state(self, client_id: int) -> None:
        """Limpiar estado del líder cuando los FLUSH_ACKs se manejan manualmente.

        Llamar bajo lock después de procesar todos los FLUSH_ACKs sin pasar por
        process_control_message(FLUSH_ACK, ...).  Necesario cuando el worker usa
        un payload de FLUSH_ACK no estándar (ej. JSON con desglose por output).
        """
        self._flush_acks.pop(client_id, None)
        self._forwarded_from_acks.pop(client_id, None)
        self._leader_expected.pop(client_id, None)
        if self._mode == "broadcast":
            self._pending_eof.pop(client_id, None)
        else:
            self._seen_eof.discard(client_id)

    # ─── API principal ────────────────────────────────────────────────────────

    def on_upstream_eof(
        self,
        client_id: int,
        expected_total: int,
        current_count: int,
        current_forwarded: int,
    ) -> Optional[CoordinatorAction]:
        """Llamar bajo lock cuando llega el EOF de upstream para client_id."""
        if self._total <= 1:
            return FlushAction(is_leader=True, total_forwarded=current_forwarded)

        if self._mode == "broadcast":
            self._leader_expected[client_id] = expected_total
            logging.info(
                "eof_coordinator_upstream_eof | id=%s | client_id=%s | expected=%s",
                self._id, client_id, expected_total,
            )
            return BroadcastAction(
                queue_names=[self.control_queue_for(i) for i in range(self._total)],
                message=self._make_packet(
                    MessageType.EOF_RECEIVED, client_id, self._id, expected_total
                ),
            )

        # flush_order: cada réplica reporta directamente al líder fijo
        if client_id in self._seen_eof:
            logging.info(
                "eof_coordinator_eof_duplicate | id=%s | client_id=%s", self._id, client_id
            )
            return None
        self._seen_eof.add(client_id)
        if self._is_leader:
            self._leader_expected[client_id] = expected_total
        logging.info(
            "eof_coordinator_report | id=%s | client_id=%s | count=%s | expected=%s",
            self._id, client_id, current_count, expected_total,
        )
        return SendAnswerAction(
            queue_name=self.response_queue_for(self._leader_id),
            message=self.build_answer(client_id, current_count, current_forwarded),
        )

    def process_control_message(
        self,
        msg_type: MessageType,
        client_id: int,
        ctrl: ControlMessage,
        current_count: int = 0,
        current_forwarded: int = 0,
    ) -> Optional[CoordinatorAction]:
        """Llamar bajo lock cuando llega cualquier mensaje de control o respuesta.

        current_count/current_forwarded solo se usan para EOF_RECEIVED y PROCESSED_REQUEST
        (toma de snapshot del conteo actual del worker).
        """
        if msg_type == MessageType.EOF_RECEIVED:
            return self._on_eof_received(client_id, ctrl, current_count, current_forwarded)
        if msg_type == MessageType.PROCESSED_REQUEST:
            return self._on_processed_request(client_id, ctrl, current_count, current_forwarded)
        if msg_type == MessageType.PROCESSED_ANSWER:
            return self._on_processed_answer(client_id, ctrl)
        if msg_type == MessageType.FLUSH_ORDER:
            return self._on_flush_order(client_id, ctrl)
        if msg_type == MessageType.FLUSH_ACK:
            return self._on_flush_ack(client_id, ctrl)
        logging.warning(
            "eof_coordinator_unknown_msg | id=%s | msg_type=%s | client_id=%s",
            self._id, msg_type, client_id,
        )
        return None

    # ─── handlers internos ───────────────────────────────────────────────────

    def _on_eof_received(
        self,
        client_id: int,
        ctrl: ControlMessage,
        current_count: int,
        current_forwarded: int,
    ) -> Optional[CoordinatorAction]:
        """Broadcast — paso 2: toma snapshot y reporta al líder."""
        leader_id = ctrl.sender_id

        if client_id in self._pending_eof:
            logging.info(
                "eof_coordinator_eof_duplicate | id=%s | client_id=%s | leader=%s",
                self._id, client_id, leader_id,
            )
            return None

        self._pending_eof[client_id] = (ctrl.expected_total, leader_id)
        logging.info(
            "eof_coordinator_snapshot | id=%s | client_id=%s | leader=%s | "
            "count=%s | fwd=%s",
            self._id, client_id, leader_id, current_count, current_forwarded,
        )
        return SendAnswerAction(
            queue_name=self.response_queue_for(leader_id),
            message=self.build_answer(client_id, current_count, current_forwarded),
        )

    def _on_processed_request(
        self,
        client_id: int,
        _ctrl: ControlMessage,
        current_count: int,
        current_forwarded: int,
    ) -> Optional[CoordinatorAction]:
        """Retry — paso 3b/2b: líder solicitó re-reporte. Enviar conteo actualizado."""
        if self._mode == "broadcast":
            entry = self._pending_eof.get(client_id)
            if entry is None:
                logging.warning(
                    "eof_coordinator_request_no_pending | id=%s | client_id=%s",
                    self._id, client_id,
                )
                return None
            leader_id = entry[1]
        else:
            leader_id = self._leader_id

        logging.info(
            "eof_coordinator_re_report | id=%s | client_id=%s | count=%s | fwd=%s",
            self._id, client_id, current_count, current_forwarded,
        )
        return SendAnswerAction(
            queue_name=self.response_queue_for(leader_id),
            message=self.build_answer(client_id, current_count, current_forwarded),
        )

    def _on_processed_answer(
        self, client_id: int, ctrl: ControlMessage
    ) -> Optional[CoordinatorAction]:
        """Líder — paso 3/2: acumula respuestas de todas las instancias.

        Espera N respuestas (incluyendo la propia del líder vía self-report).
        Cuando llegan todas: verifica total >= expected o reintenta.
        """
        sender_id = ctrl.sender_id

        if client_id not in self._leader_responders:
            self._leader_responders[client_id] = set()
        if sender_id in self._leader_responders[client_id]:
            logging.info(
                "eof_coordinator_answer_duplicate | id=%s | client_id=%s | sender=%s",
                self._id, client_id, sender_id,
            )
            return None
        self._leader_responders[client_id].add(sender_id)
        self._leader_processed[client_id] = (
            self._leader_processed.get(client_id, 0) + ctrl.processed_count
        )

        # In flush_order mode, non-leaders can send PROCESSED_ANSWER before the leader has
        # processed its own shard EOF (which sets _leader_expected). Always accumulate; defer
        # the flush decision until both all N responders and the expected total are known.
        expected = self._leader_expected.get(client_id)
        responder_count = len(self._leader_responders[client_id])
        running = self._leader_processed[client_id]

        logging.info(
            "eof_coordinator_accumulate | id=%s | client_id=%s | "
            "sender=%s | responders=%s/%s | running=%s | expected=%s",
            self._id, client_id, sender_id, responder_count, self._total, running,
            expected if expected is not None else "?",
        )

        if expected is None or responder_count < self._total:
            return None  # Esperando expected total o respuestas faltantes

        # Todas las instancias respondieron — evaluar conteo
        if running >= expected:
            # Conteos correctos: broadcast FLUSH_ORDER
            self._leader_responders.pop(client_id, None)
            self._leader_processed.pop(client_id, None)
            # Conservar _leader_expected hasta recibir todos los FLUSH_ACKs
            logging.info(
                "eof_coordinator_counts_ok | id=%s | client_id=%s | sending FLUSH_ORDER",
                self._id, client_id,
            )
            return BroadcastAction(
                queue_names=[self.control_queue_for(i) for i in range(self._total)],
                message=self._make_packet(MessageType.FLUSH_ORDER, client_id, self._id, 0),
            )

        # Conteos insuficientes: reintentar con PROCESSED_REQUEST
        self._leader_responders[client_id] = set()
        self._leader_processed[client_id] = 0
        logging.info(
            "eof_coordinator_counts_mismatch | id=%s | client_id=%s | "
            "running=%s | expected=%s | retrying",
            self._id, client_id, running, expected,
        )
        return BroadcastAction(
            queue_names=[self.control_queue_for(i) for i in range(self._total)],
            message=self._make_packet(
                MessageType.PROCESSED_REQUEST, client_id, self._id, expected
            ),
            sleep_before=0.5,
        )

    def _on_flush_order(
        self, client_id: int, ctrl: ControlMessage
    ) -> Optional[CoordinatorAction]:
        """Paso 4: réplica recibe FLUSH_ORDER del líder."""
        leader_id = ctrl.sender_id

        # ¿Soy el líder para este client_id?
        is_leader_here = (
            client_id in self._leader_expected
            if self._mode == "broadcast"
            else self._is_leader
        )

        if is_leader_here:
            logging.warning(
                "eof_coordinator_flush_order_on_leader | id=%s | client_id=%s",
                self._id, client_id,
            )
            return None

        logging.info(
            "eof_coordinator_flush_order | id=%s | client_id=%s | leader=%s",
            self._id, client_id, leader_id,
        )
        return FlushAction(
            is_leader=False,
            ack_queue=self.response_queue_for(leader_id),
        )

    def _on_flush_ack(
        self, client_id: int, ctrl: ControlMessage
    ) -> Optional[CoordinatorAction]:
        """Líder — paso 5/4: acumula FLUSH_ACKs. Cuando llegan N-1, retorna FlushAction."""
        is_leader_here = (
            client_id in self._leader_expected
            if self._mode == "broadcast"
            else self._is_leader
        )

        if not is_leader_here:
            logging.warning(
                "eof_coordinator_flush_ack_on_non_leader | id=%s | client_id=%s",
                self._id, client_id,
            )
            return None

        sender_id = ctrl.sender_id
        if client_id not in self._flush_acks:
            self._flush_acks[client_id] = set()
        self._flush_acks[client_id].add(sender_id)
        self._forwarded_from_acks[client_id] = (
            self._forwarded_from_acks.get(client_id, 0) + ctrl.processed_count
        )

        ack_count = len(self._flush_acks[client_id])
        logging.info(
            "eof_coordinator_flush_ack | id=%s | client_id=%s | "
            "sender=%s | acks=%s/%s | fwd_so_far=%s",
            self._id, client_id, sender_id, ack_count, self._total - 1,
            self._forwarded_from_acks[client_id],
        )

        if ack_count < self._total - 1:
            return None  # Faltan ACKs

        # Todos los no-líderes hicieron flush
        total_forwarded = self._forwarded_from_acks.pop(client_id, 0)
        self._flush_acks.pop(client_id, None)
        self._leader_expected.pop(client_id, None)
        if self._mode == "broadcast":
            self._pending_eof.pop(client_id, None)
        else:
            self._seen_eof.discard(client_id)

        logging.info(
            "eof_coordinator_all_flush_acks | id=%s | client_id=%s | total_fwd=%s",
            self._id, client_id, total_forwarded,
        )
        return FlushAction(is_leader=True, total_forwarded=total_forwarded)

    # ─── read-only accessors (for business_fn prediction) ───────────────────
    # These allow callers to predict the outcome of a coordinator call WITHOUT
    # making a mutating call, which is required when coordinator methods are
    # non-idempotent (e.g. += accumulation in _on_processed_answer/_on_flush_ack).
    # All are safe to call under the worker's main lock.

    def responder_count(self, client_id: int) -> int:
        """Number of distinct senders that have sent PROCESSED_ANSWER for client_id."""
        return len(self._leader_responders.get(client_id, set()))

    def has_responder(self, client_id: int, sender_id: int) -> bool:
        return sender_id in self._leader_responders.get(client_id, set())

    def accumulated_processed(self, client_id: int) -> int:
        """Sum of processed_count from all PROCESSED_ANSWERs received so far."""
        return self._leader_processed.get(client_id, 0)

    def leader_expected(self, client_id: int) -> int | None:
        """Expected total from the upstream EOF, or None if not yet received."""
        return self._leader_expected.get(client_id)

    def flush_ack_count(self, client_id: int) -> int:
        """Number of distinct non-leaders that have sent FLUSH_ACK for client_id."""
        return len(self._flush_acks.get(client_id, set()))

    def has_flush_ack(self, client_id: int, sender_id: int) -> bool:
        return sender_id in self._flush_acks.get(client_id, set())

    def forwarded_from_acks(self, client_id: int) -> int:
        """Sum of forwarded counts from all FLUSH_ACKs received so far."""
        return self._forwarded_from_acks.get(client_id, 0)

    def accumulated_forwarded(self, client_id: int) -> int:
        """Sum of forwarded counts from all FLUSH_ACKs received so far."""
        return self._forwarded_from_acks.get(client_id, 0)

    # ─── snapshot / restore ──────────────────────────────────────────────────

    def snapshot(self) -> dict:
        """Return a picklable copy of all mutable coordinator state.
        Caller must hold the worker's main lock."""
        return {
            "pending_eof":         dict(self._pending_eof),
            "seen_eof":            set(self._seen_eof),
            "leader_expected":     dict(self._leader_expected),
            "leader_processed":    dict(self._leader_processed),
            "leader_responders":   {k: set(v) for k, v in self._leader_responders.items()},
            "flush_acks":          {k: set(v) for k, v in self._flush_acks.items()},
            "forwarded_from_acks": dict(self._forwarded_from_acks),
        }

    def restore(self, snap: dict) -> None:
        """Restore coordinator state from a snapshot dict.
        Caller must hold the worker's main lock."""
        self._pending_eof          = snap["pending_eof"]
        self._seen_eof             = snap["seen_eof"]
        self._leader_expected      = snap["leader_expected"]
        self._leader_processed     = snap["leader_processed"]
        self._leader_responders    = snap["leader_responders"]
        self._flush_acks           = snap["flush_acks"]
        self._forwarded_from_acks  = snap["forwarded_from_acks"]

    # ─── construcción de paquetes ─────────────────────────────────────────────

    def _make_packet(
        self,
        msg_type: MessageType,
        client_id: int,
        sender_id: int,
        expected_total: int,
        processed_count: int = 0,
    ) -> bytes:
        payload = self._ctrl_ser.serialize(
            ControlMessage(
                sender_id=sender_id,
                expected_total=expected_total,
                processed_count=processed_count,
            )
        )
        return self._proto.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
