import base64
import copy
import logging
import threading
from dataclasses import dataclass

from common import middleware
from common.bank_ids import notebook_bank_id
from common.domain.account import Q2BankMaxResult
from common.domain.partial_result import Q2BankMaxPartial
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import (
    PersistentStateHandler,
)
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
from workers.q2_bank_name_joiner.bank_name_joiner_state import (
    BankNameJoinerState,
    ClientState,
)


ACCOUNTS_SENDER_OFFSET = 1_000_000


@dataclass(frozen=True)
class BankNameJoinerConfig:
    id: int
    mom_host: str
    q2_input_queue: str
    accounts_input_queue: str
    output_queue: str
    state_dir: str
    snapshot_interval: int


class BankNameJoinerWorker:
    def __init__(self, config: BankNameJoinerConfig) -> None:
        self._config = config

        self._q2_consumer: middleware.MessageMiddlewareQueueRabbitMQ | None = None
        self._accounts_consumer: middleware.MessageMiddlewareQueueRabbitMQ | None = None
        self._output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host, self._config.output_queue
        )

        self._protocol = InternalProtocol()
        self._control_serializer = ControlMessageSerializer()
        self._line_batch_serializer = LineBatchSerializer()

        self._lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._state = BankNameJoinerState()
        self._handler = PersistentStateHandler(
            state_dir=self._config.state_dir,
            node_id=f"q2_bank_name_joiner_{self._config.id}",
            worker_state=self._state,
            snapshot_every=self._config.snapshot_interval,
        )

        self._closed = False
        self._stopped = False

        self._q2_thread: threading.Thread | None = None
        self._accounts_thread: threading.Thread | None = None

        self._handler.recover()
        self._republish_pending()

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
        for resource in (self._q2_consumer, self._accounts_consumer, self._output_queue):
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "q2_bank_name_joiner_close_error | id=%s | error=%s",
                    self._config.id, e,
                )

    def _republish_pending(self) -> None:
        for entry in self._handler.outbox_to_republish():
            try:
                self._send_output(entry.body)
            except Exception:
                logging.exception(
                    "q2_bank_name_joiner_republish_error | id=%s | destination=%s",
                    self._config.id,
                    entry.destination,
                )
                raise

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
            try:
                self._accounts_consumer.close()
            except Exception:
                pass

    def _on_q2_message(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self._protocol.unpack_addressed_packet(message)
            )
            msg_type = MessageType(msg_type)
            msg_id = f"q2:{sender_id}:{client_id}:{seq}"

            with self._lock:
                if msg_type == MessageType.DATA:
                    def bfn(pl: bytes):
                        change = BankNameJoinerState.q2_data_change(client_id, pl)
                        return self._change_outputs_for(client_id, change)

                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, payload, bfn
                    )
                elif msg_type == MessageType.EOF:
                    control = self._control_serializer.deserialize(payload)

                    def bfn(_pl: bytes):
                        change = BankNameJoinerState.q2_eof_change(
                            client_id, control.expected_total
                        )
                        return self._change_outputs_for(client_id, change)

                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, payload, bfn
                    )
                    logging.info(
                        "q2_bank_name_joiner_q2_eof | id=%s | client_id=%s | "
                        "expected_total=%s",
                        self._config.id, client_id, control.expected_total,
                    )
                else:
                    raise ValueError(f"unsupported q2 message type: {msg_type}")

            committed = self._publish_commit_ack(instruction, ack)
            self._log_if_emitted(client_id, instruction, committed)
        except Exception as e:
            logging.error(
                "q2_bank_name_joiner_q2_error | id=%s | error=%s",
                self._config.id, e,
                exc_info=True,
            )
            nack(requeue=True)

    def _on_accounts_message(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self._protocol.unpack_addressed_packet(message)
            )
            msg_type = MessageType(msg_type)
            effective_sender_id = sender_id + ACCOUNTS_SENDER_OFFSET
            msg_id = f"acc:{sender_id}:{client_id}:{seq}"

            if msg_type == MessageType.DATA:
                mappings = self._parse_accounts_batch(payload)
                with self._lock:
                    def bfn(_pl: bytes):
                        change = BankNameJoinerState.accounts_data_change(
                            client_id, mappings
                        )
                        return self._change_outputs_for(client_id, change)

                    instruction = self._handler.handle(
                        msg_id,
                        client_id,
                        effective_sender_id,
                        seq,
                        payload,
                        bfn,
                    )
            elif msg_type == MessageType.EOF:
                control = self._control_serializer.deserialize(payload)
                with self._lock:
                    def bfn(_pl: bytes):
                        change = BankNameJoinerState.accounts_eof_change(
                            client_id, control.expected_total
                        )
                        return self._change_outputs_for(client_id, change)

                    instruction = self._handler.handle(
                        msg_id,
                        client_id,
                        effective_sender_id,
                        seq,
                        payload,
                        bfn,
                    )
                    logging.info(
                        "q2_bank_name_joiner_accounts_eof | id=%s | client_id=%s | "
                        "expected_total=%s",
                        self._config.id, client_id, control.expected_total,
                    )
            else:
                raise ValueError(f"unsupported accounts message type: {msg_type}")

            committed = self._publish_commit_ack(instruction, ack)
            self._log_if_emitted(client_id, instruction, committed)
        except Exception as e:
            logging.error(
                "q2_bank_name_joiner_accounts_error | id=%s | error=%s",
                self._config.id, e,
                exc_info=True,
            )
            nack(requeue=True)

    def _change_outputs_for(self, client_id: int, change: dict) -> tuple[dict, list]:
        prospective = self._prospective_state(client_id, change)
        if not prospective.ready():
            return change, []
        return (
            BankNameJoinerState.close_change(client_id),
            self._build_result_outputs(prospective, client_id),
        )

    def _prospective_state(self, client_id: int, change: dict) -> ClientState:
        current = self._state.client_state(client_id)
        state = copy.deepcopy(current) if current is not None else ClientState()
        kind = change["type"]
        if kind == "q2_data":
            partial = Q2BankMaxPartialSerializer.deserialize(
                self._decode_payload(change["payload_b64"])
            )
            bank_id = notebook_bank_id(partial.bank_id)
            if bank_id != partial.bank_id:
                partial = Q2BankMaxPartial(
                    bank_id=bank_id,
                    from_account=partial.from_account,
                    amount=partial.amount,
                )
            state.q2_results[bank_id] = partial
            state.q2_data_count += 1
        elif kind == "q2_eof":
            state.q2_expected_total = change["expected_total"]
        elif kind == "accounts_data":
            for bank_id, bank_name in change["mappings"]:
                bank_names = state.bank_names.setdefault(bank_id, [])
                if bank_name not in bank_names:
                    bank_names.append(bank_name)
            state.accounts_batch_count += 1
        elif kind == "accounts_eof":
            state.accounts_expected_total = change["expected_total"]
        else:
            raise ValueError(f"unsupported prospective change type: {kind}")
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
            bank_id = notebook_bank_id(fields[bank_id_idx])
            bank_name = (fields[bank_name_idx] or "").strip()
            if not bank_id:
                continue
            mappings.append((bank_id, bank_name))
        return mappings

    def _build_result_outputs(
        self, state: ClientState, client_id: int
    ) -> list[tuple[str, bytes]]:
        outputs: list[tuple[str, bytes]] = []
        emitted = 0
        for bank_id, partial in state.q2_results.items():
            bank_names = state.bank_names.get(bank_id, [])
            for bank_name in bank_names:
                result = Q2BankMaxResult(
                    bank_id=partial.bank_id,
                    from_account=partial.from_account,
                    bank_name=bank_name,
                    amount=partial.amount,
                )
                payload = Q2BankMaxResultSerializer.serialize(result)
                outputs.append((
                    self._config.output_queue,
                    self._packet(MessageType.DATA, client_id, emitted, payload),
                ))
                emitted += 1

        control_payload = self._control_serializer.serialize(
            ControlMessage(
                sender_id=self._config.id,
                expected_total=emitted,
                processed_count=0,
            )
        )
        outputs.append((
            self._config.output_queue,
            self._packet(MessageType.EOF, client_id, emitted, control_payload),
        ))
        return outputs

    def _publish_commit_ack(self, instruction, ack) -> bool:
        if instruction.action is Action.ACK:
            ack()
            return False
        for entry in instruction.outputs:
            self._send_output(entry.body)
        with self._lock:
            self._handler.commit_done(*instruction.ctx)
        ack()
        return True

    def _send_output(self, body: bytes) -> None:
        with self._publish_lock:
            self._output_queue.send(body)

    def _packet(
        self, msg_type: MessageType, client_id: int, seq: int, payload: bytes
    ) -> bytes:
        return self._protocol.create_addressed_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            sender_id=self._config.id,
            seq=seq,
            payload=payload,
        )

    def _log_if_emitted(self, client_id: int, instruction, committed: bool) -> None:
        if not committed or not instruction.outputs:
            return
        results = max(len(instruction.outputs) - 1, 0)
        logging.info(
            "q2_bank_name_joiner_emit | id=%s | client_id=%s | results=%s",
            self._config.id, client_id, results,
        )

    @staticmethod
    def _decode_payload(payload_b64: str) -> bytes:
        return base64.b64decode(payload_b64)
