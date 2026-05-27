import logging
import os
import threading
import time
from dataclasses import dataclass, field

from common import middleware
from common.domain.account import Q2BankMaxResult
from common.domain.partial_result import Q2BankMaxPartial
from common.logging_utils import should_log_progress
from common.message_protocol.external.types import FILE_TYPE_ACCOUNTS
from common.message_protocol.internal import (
    InternalProtocol,
    LineBatchSerializer,
    Q2BankMaxPartialSerializer,
    Q2BankMaxResultSerializer,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from workers.common.line_splitter import parse_csv_line


MAX_CLOSED_CLIENTS = 500


def _normalize_bank_id(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if value.isdigit():
        return str(int(value))
    return value.lstrip("0") or "0"


@dataclass
class ClientState:
    bank_names: dict[str, str] = field(default_factory=dict)
    q2_results: dict[str, Q2BankMaxPartial] = field(default_factory=dict)
    q2_data_count: int = 0
    q2_expected_total: int | None = None
    accounts_batch_count: int = 0
    accounts_expected_total: int | None = None

    def ready(self) -> bool:
        if self.q2_expected_total is None or self.accounts_expected_total is None:
            return False
        if self.q2_data_count < self.q2_expected_total:
            return False
        if self.accounts_batch_count < self.accounts_expected_total:
            return False
        return True


@dataclass(frozen=True)
class BankNameJoinerConfig:
    id: int
    mom_host: str
    q2_input_queue: str
    accounts_input_queue: str
    output_queue: str


class BankNameJoinerWorker:
    def __init__(self, config: BankNameJoinerConfig) -> None:
        self._config = config

        self._q2_consumer: middleware.MessageMiddlewareQueueRabbitMQ | None = None
        self._accounts_consumer: middleware.MessageMiddlewareQueueRabbitMQ | None = None

        self._protocol = InternalProtocol()
        self._control_serializer = ControlMessageSerializer()
        self._line_batch_serializer = LineBatchSerializer()

        self._lock = threading.Lock()
        self._output_queues_lock = threading.Lock()
        self._states: dict[int, ClientState] = {}
        self._closed_by_client: dict[int, float] = {}
        self._output_queues_by_thread: dict[
            int, middleware.MessageMiddlewareQueueRabbitMQ
        ] = {}
        self._closed = False
        self._stopped = False

        self._q2_thread: threading.Thread | None = None
        self._accounts_thread: threading.Thread | None = None

    def start(self) -> None:
        logging.info(
            "q2_bank_name_joiner_start | id=%s | q2_input=%s | accounts_input=%s | "
            "output=%s",
            self._config.id,
            self._config.q2_input_queue,
            self._config.accounts_input_queue,
            self._config.output_queue,
        )

        self._q2_thread = threading.Thread(target=self._run_q2_consumer, daemon=True)
        self._accounts_thread = threading.Thread(
            target=self._run_accounts_consumer, daemon=True
        )
        self._q2_thread.start()
        self._accounts_thread.start()
        self._await_consumers()

    def _await_consumers(self) -> None:
        threads = [t for t in (self._q2_thread, self._accounts_thread) if t is not None]
        while True:
            alive = [t for t in threads if t.is_alive()]
            if not alive:
                return
            for thread in alive:
                thread.join(timeout=1.0)
            if self._stopped:
                for thread in alive:
                    thread.join(timeout=5.0)
                return

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        logging.info("q2_bank_name_joiner_stop | id=%s", self._config.id)
        for consumer in (self._q2_consumer, self._accounts_consumer):
            if consumer is not None:
                consumer.request_stop_consuming()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._close_thread_output_queue()

    def _output_queue_for_thread(self) -> middleware.MessageMiddlewareQueueRabbitMQ:
        thread_id = threading.get_ident()
        with self._output_queues_lock:
            output_queue = self._output_queues_by_thread.get(thread_id)
            if output_queue is None:
                output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                    self._config.mom_host, self._config.output_queue
                )
                self._output_queues_by_thread[thread_id] = output_queue
            return output_queue

    def _close_thread_output_queue(self) -> None:
        thread_id = threading.get_ident()
        with self._output_queues_lock:
            output_queue = self._output_queues_by_thread.pop(thread_id, None)
        if output_queue is None:
            return
        try:
            output_queue.close()
        except Exception as e:
            logging.warning(
                "q2_bank_name_joiner_close_error | id=%s | error=%s",
                self._config.id, e,
            )

    def _run_q2_consumer(self) -> None:
        self._q2_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host, self._config.q2_input_queue
        )
        try:
            if not self._stopped:
                self._q2_consumer.start_consuming(self._on_q2_message)
        except Exception as e:
            if not self._closed:
                logging.error(
                    "q2_bank_name_joiner_q2_consumer_error | id=%s | error=%s",
                    self._config.id, e,
                )
        finally:
            self._close_thread_output_queue()
            try:
                self._q2_consumer.close()
            except Exception:
                pass

    def _run_accounts_consumer(self) -> None:
        self._accounts_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host, self._config.accounts_input_queue
        )
        try:
            if not self._stopped:
                self._accounts_consumer.start_consuming(self._on_accounts_message)
        except Exception as e:
            if not self._closed:
                logging.error(
                    "q2_bank_name_joiner_accounts_consumer_error | id=%s | error=%s",
                    self._config.id, e,
                )
        finally:
            self._close_thread_output_queue()
            try:
                self._accounts_consumer.close()
            except Exception:
                pass

    def _on_q2_message(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, payload = self._protocol.unpack_packet(message)
            should_emit = False

            with self._lock:
                if client_id in self._closed_by_client:
                    logging.info(
                        "q2_bank_name_joiner_q2_for_closed_client | id=%s | "
                        "client_id=%s | msg_type=%s",
                        self._config.id, client_id, msg_type,
                    )
                    ack()
                    return

                state = self._state_for(client_id)

                if msg_type == MessageType.DATA:
                    partial = Q2BankMaxPartialSerializer.deserialize(payload)
                    state.q2_results[partial.bank_id] = partial
                    state.q2_data_count += 1
                    if should_log_progress(state.q2_data_count):
                        logging.info(
                            "q2_bank_name_joiner_q2_data | id=%s | client_id=%s | "
                            "data_count=%s | banks=%s | payload_bytes=%s",
                            self._config.id,
                            client_id,
                            state.q2_data_count,
                            len(state.q2_results),
                            len(payload),
                        )
                elif msg_type == MessageType.EOF:
                    control = self._control_serializer.deserialize(payload)
                    state.q2_expected_total = control.expected_total
                    logging.info(
                        "q2_bank_name_joiner_q2_eof | id=%s | client_id=%s | "
                        "data_count=%s | expected_total=%s",
                        self._config.id, client_id,
                        state.q2_data_count, state.q2_expected_total,
                    )
                else:
                    raise ValueError(f"unsupported q2 message type: {msg_type}")

                should_emit = state.ready()

            if should_emit:
                self._emit_results(client_id)
            ack()
        except Exception as e:
            logging.error(
                "q2_bank_name_joiner_q2_error | id=%s | error=%s",
                self._config.id, e,
            )
            nack()

    def _on_accounts_message(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, payload = self._protocol.unpack_packet(message)
            should_emit = False

            if msg_type == MessageType.DATA:
                mappings = self._parse_accounts_batch(payload)
                with self._lock:
                    if client_id in self._closed_by_client:
                        ack()
                        return
                    state = self._state_for(client_id)
                    for bank_id, bank_name in mappings:
                        state.bank_names.setdefault(bank_id, bank_name)
                    state.accounts_batch_count += 1
                    if should_log_progress(state.accounts_batch_count):
                        logging.info(
                            "q2_bank_name_joiner_accounts_data | id=%s | "
                            "client_id=%s | batches=%s | mappings_in_batch=%s | "
                            "known_banks=%s | payload_bytes=%s",
                            self._config.id,
                            client_id,
                            state.accounts_batch_count,
                            len(mappings),
                            len(state.bank_names),
                            len(payload),
                        )
                    should_emit = state.ready()

            elif msg_type == MessageType.EOF:
                control = self._control_serializer.deserialize(payload)
                with self._lock:
                    if client_id in self._closed_by_client:
                        ack()
                        return
                    state = self._state_for(client_id)
                    state.accounts_expected_total = control.expected_total
                    logging.info(
                        "q2_bank_name_joiner_accounts_eof | id=%s | client_id=%s | "
                        "batch_count=%s | expected_total=%s | mappings=%s",
                        self._config.id, client_id,
                        state.accounts_batch_count,
                        state.accounts_expected_total,
                        len(state.bank_names),
                    )
                    should_emit = state.ready()
            else:
                raise ValueError(f"unsupported accounts message type: {msg_type}")

            if should_emit:
                self._emit_results(client_id)
            ack()
        except Exception as e:
            logging.error(
                "q2_bank_name_joiner_accounts_error | id=%s | error=%s",
                self._config.id, e,
            )
            nack()

    def _state_for(self, client_id: int) -> ClientState:
        state = self._states.get(client_id)
        if state is None:
            state = ClientState()
            self._states[client_id] = state
        return state

    def _parse_accounts_batch(self, payload: bytes) -> list[tuple[str, str]]:
        batch = self._line_batch_serializer.deserialize(payload)
        if batch.file_type != FILE_TYPE_ACCOUNTS:
            raise ValueError(
                f"unexpected accounts batch file_type: {batch.file_type}"
            )

        header = list(batch.header)
        try:
            bank_id_idx = header.index("Bank ID")
            bank_name_idx = header.index("Bank Name")
        except ValueError as exc:
            raise ValueError(f"accounts header missing required columns: {header}") from exc

        mappings: list[tuple[str, str]] = []
        for line in batch.lines:
            clean = line[:-1] if line.endswith(b"\r") else line
            if not clean:
                continue
            fields = parse_csv_line(clean)
            if len(fields) <= max(bank_id_idx, bank_name_idx):
                continue
            bank_id = _normalize_bank_id(fields[bank_id_idx])
            bank_name = (fields[bank_name_idx] or "").strip()
            if not bank_id:
                continue
            mappings.append((bank_id, bank_name))
        return mappings

    def _emit_results(self, client_id: int) -> None:
        with self._lock:
            state = self._states.pop(client_id, None)
            self._closed_by_client[client_id] = time.monotonic()
            if len(self._closed_by_client) > MAX_CLOSED_CLIENTS:
                self._cleanup_closed_clients()

        if state is None:
            return

        output_queue = self._output_queue_for_thread()
        missing = 0
        emitted = 0
        for bank_id, partial in state.q2_results.items():
            normalized = _normalize_bank_id(bank_id)
            bank_name = state.bank_names.get(normalized, "")
            if not bank_name:
                missing += 1
            result = Q2BankMaxResult(
                bank_id=partial.bank_id,
                from_account=partial.from_account,
                bank_name=bank_name,
                amount=partial.amount,
            )
            payload = Q2BankMaxResultSerializer.serialize(result)
            output_queue.send(
                self._packet(MessageType.DATA, client_id, payload)
            )
            emitted += 1

        control_payload = self._control_serializer.serialize(
            ControlMessage(
                sender_id=self._config.id,
                expected_total=emitted,
                processed_count=0,
            )
        )
        output_queue.send(self._packet(MessageType.EOF, client_id, control_payload))

        logging.info(
            "q2_bank_name_joiner_emit | id=%s | client_id=%s | results=%s | "
            "bank_names=%s | unmatched=%s",
            self._config.id, client_id, emitted, len(state.bank_names), missing,
        )

    def _cleanup_closed_clients(self) -> None:
        items = sorted(self._closed_by_client.items(), key=lambda item: item[1])
        to_remove = len(items) - MAX_CLOSED_CLIENTS // 2
        for client_id, _ in items[:to_remove]:
            self._closed_by_client.pop(client_id, None)

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
