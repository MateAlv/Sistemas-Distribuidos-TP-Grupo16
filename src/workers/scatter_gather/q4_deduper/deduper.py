import logging
import os
import threading
from collections import defaultdict

from common.batch_buffer import BatchBuffer
from common.logging_utils import should_log_progress
from common.message_protocol.internal import (
    InternalProtocol,
    Q4AccountId,
    Q4AccountIdSerializer,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
Q4_DEDUPER_EXCHANGE = os.environ["Q4_DEDUPER_EXCHANGE"]
Q4_DEDUPER_ROUTING_PREFIX = os.environ.get(
    "Q4_DEDUPER_ROUTING_PREFIX", "q4_deduper"
)
Q4_AGGREGATOR_AMOUNT = int(os.environ["Q4_AGGREGATOR_AMOUNT"])
Q4_DEDUPER_AMOUNT = int(os.environ["Q4_DEDUPER_AMOUNT"])
Q4_DEDUPER_RESPONSE_QUEUE_PREFIX = os.environ.get(
    "Q4_DEDUPER_RESPONSE_QUEUE_PREFIX",
    "q4_deduper_response",
)
GATEWAY_Q4_QUEUE = os.environ["GATEWAY_Q4_QUEUE"]
Q4_DEDUPER_BATCH_BYTES = int(
    os.environ.get("Q4_DEDUPER_BATCH_BYTES", str(1024 * 1024))
)
Q4_DEDUPER_BATCH_MAX_ACCOUNTS = int(
    os.environ.get("Q4_DEDUPER_BATCH_MAX_ACCOUNTS", "5000")
)

class Q4DeduperWorker:
    """Deduplicates notebook Q4 account candidates and writes gateway batches."""

    def __init__(self):
        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_DEDUPER_EXCHANGE,
            [self._input_routing_key()],
            queue_name=self._input_routing_key(),
            exclusive=False,
        )
        self._gateway_output = self._new_gateway_output()
        self._leader_output = (
            self._new_leader_output()
            if Q4_DEDUPER_AMOUNT > 1
            else None
        )
        self._response_consumer = (
            self._new_response_consumer()
            if Q4_DEDUPER_AMOUNT > 1 and ID == 0
            else None
        )
        self._gateway_eof_output = (
            self._new_gateway_output()
            if Q4_DEDUPER_AMOUNT > 1 and ID == 0
            else None
        )

        self._proto = InternalProtocol()
        self._account_serializer = Q4AccountIdSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._batcher = BatchBuffer(
            Q4_DEDUPER_BATCH_BYTES,
            Q4_DEDUPER_BATCH_MAX_ACCOUNTS,
        )

        self._lock = threading.Lock()
        self._accounts_by_client: dict[int, set[tuple[str, str]]] = {}
        self._processed_by_client: dict[int, int] = {}
        self._eofs_by_client: dict[int, set[int]] = defaultdict(set)
        self._closed_by_client: set[int] = set()

        self._leader_reports_by_client: dict[int, set[int]] = defaultdict(set)
        self._leader_totals_by_client: dict[int, int] = {}
        self._leader_closed_by_client: set[int] = set()

        self._response_thread = None
        self._closed = False
        self._stopped = False

    def _input_routing_key(self) -> str:
        return f"{Q4_DEDUPER_ROUTING_PREFIX}_{ID}"

    def _response_queue_name(self, worker_id: int = 0) -> str:
        return f"{Q4_DEDUPER_RESPONSE_QUEUE_PREFIX}_{worker_id}"

    def _new_gateway_output(self):
        return MessageMiddlewareQueueRabbitMQ(MOM_HOST, GATEWAY_Q4_QUEUE)

    def _new_leader_output(self):
        return MessageMiddlewareQueueRabbitMQ(MOM_HOST, self._response_queue_name(0))

    def _new_response_consumer(self):
        return MessageMiddlewareQueueRabbitMQ(MOM_HOST, self._response_queue_name(ID))

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._proto.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _control_payload(
        self,
        sender_id: int,
        expected_total: int,
        processed_count: int,
    ) -> bytes:
        return self._control_serializer.serialize(
            ControlMessage(
                sender_id=sender_id,
                expected_total=expected_total,
                processed_count=processed_count,
            )
        )

    def _account_key(self, account: Q4AccountId) -> tuple[str, str]:
        return (account.bank_id, account.account)

    def _account_from_key(self, key: tuple[str, str]) -> Q4AccountId:
        return Q4AccountId(bank_id=key[0], account=key[1])

    def _send_gateway_batch(
        self,
        client_id: int,
        batch_payload: bytes,
        output=None,
    ) -> None:
        output = output or self._gateway_output
        output.send(self._packet(MessageType.DATA, client_id, batch_payload))

    def _append_gateway_account(
        self,
        client_id: int,
        account: Q4AccountId,
        output=None,
    ) -> None:
        payload = self._account_serializer.serialize(account)
        batch_payload = self._batcher.append(client_id, payload)
        if batch_payload is not None:
            self._send_gateway_batch(client_id, batch_payload, output)

    def _flush_gateway_buffers(self, client_id: int, output=None) -> None:
        for _, batch_payload in self._batcher.flush(lambda k: k == client_id):
            self._send_gateway_batch(client_id, batch_payload, output)

    def _send_gateway_eof(
        self,
        client_id: int,
        expected_total: int,
        output=None,
    ) -> None:
        output = output or self._gateway_output
        output.send(
            self._packet(
                MessageType.EOF,
                client_id,
                self._control_payload(
                    sender_id=ID,
                    expected_total=expected_total,
                    processed_count=0,
                ),
            )
        )
        logging.info(
            "q4_deduper_forward_gateway_eof | id=%s | client_id=%s | "
            "expected_total=%s",
            ID,
            client_id,
            expected_total,
        )

    def _report_to_leader(
        self,
        client_id: int,
        emitted_accounts: int,
        output=None,
    ) -> None:
        output = output or self._leader_output
        if output is None:
            raise RuntimeError("leader output is not configured")
        output.send(
            self._packet(
                MessageType.EOF_RECEIVED,
                client_id,
                self._control_payload(
                    sender_id=ID,
                    expected_total=emitted_accounts,
                    processed_count=0,
                ),
            )
        )
        logging.info(
            "q4_deduper_report_leader | id=%s | client_id=%s | "
            "emitted_accounts=%s",
            ID,
            client_id,
            emitted_accounts,
        )

    def _iter_unique_accounts(self, keys: set[tuple[str, str]]):
        for key in sorted(keys):
            yield self._account_from_key(key)

    def _accept_accounts(self, client_id: int, payload: bytes) -> int:
        accounts = self._account_serializer.deserialize_batch(payload)
        if not accounts:
            return 0

        with self._lock:
            if client_id in self._closed_by_client:
                logging.info(
                    "q4_deduper_message_for_closed_client | id=%s | "
                    "client_id=%s",
                    ID,
                    client_id,
                )
                return 0

            keys = self._accounts_by_client.setdefault(client_id, set())
            for account in accounts:
                keys.add(self._account_key(account))

            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + len(accounts)
            )
            processed_total = self._processed_by_client[client_id]
            unique_in_memory = len(self._accounts_by_client.get(client_id, set()))

        if should_log_progress(processed_total):
            logging.info(
                "q4_deduper_data_batch | id=%s | client_id=%s | "
                "batch_size=%s | processed_total=%s | in_memory_unique=%s",
                ID,
                client_id,
                len(accounts),
                processed_total,
                unique_in_memory,
            )
        return len(accounts)

    def _handle_eof(self, client_id: int, payload: bytes) -> None:
        control = self._control_serializer.deserialize(payload)
        should_emit = False

        with self._lock:
            if client_id in self._closed_by_client:
                return
            if control.sender_id in self._eofs_by_client[client_id]:
                logging.info(
                    "q4_deduper_duplicate_eof | id=%s | client_id=%s | "
                    "pair_reducer_id=%s",
                    ID,
                    client_id,
                    control.sender_id,
                )
                return

            self._eofs_by_client[client_id].add(control.sender_id)
            eof_count = len(self._eofs_by_client[client_id])
            processed_total = self._processed_by_client.get(client_id, 0)
            if eof_count >= Q4_AGGREGATOR_AMOUNT:
                should_emit = True

        logging.info(
            "q4_deduper_eof_received | id=%s | client_id=%s | "
            "pair_reducer_id=%s | eof_count=%s | expected_eofs=%s | "
            "sender_expected_total=%s | processed_total=%s",
            ID,
            client_id,
            control.sender_id,
            eof_count,
            Q4_AGGREGATOR_AMOUNT,
            control.expected_total,
            processed_total,
        )

        if should_emit:
            self._emit_and_close_client(client_id)

    def _emit_and_close_client(self, client_id: int, output=None) -> int:
        with self._lock:
            if client_id in self._closed_by_client:
                return 0
            keys = self._accounts_by_client.pop(client_id, set())
            processed_total = self._processed_by_client.pop(client_id, 0)
            self._eofs_by_client.pop(client_id, None)
            self._closed_by_client.add(client_id)

        emitted_accounts = 0
        try:
            for account in self._iter_unique_accounts(keys):
                self._append_gateway_account(client_id, account, output)
                emitted_accounts += 1
            self._flush_gateway_buffers(client_id, output)
        finally:
            self._batcher.discard(lambda k: k == client_id)

        logging.info(
            "q4_deduper_emit_client | id=%s | client_id=%s | "
            "processed_total=%s | emitted_accounts=%s",
            ID,
            client_id,
            processed_total,
            emitted_accounts,
        )

        if Q4_DEDUPER_AMOUNT == 1:
            self._send_gateway_eof(client_id, emitted_accounts, output)
        else:
            self._report_to_leader(client_id, emitted_accounts)
        return emitted_accounts

    def _handle_leader_report(
        self,
        client_id: int,
        control: ControlMessage,
        output=None,
    ) -> None:
        if ID != 0:
            return

        should_forward = False
        with self._lock:
            if client_id in self._leader_closed_by_client:
                return
            if control.sender_id in self._leader_reports_by_client[client_id]:
                logging.info(
                    "q4_deduper_duplicate_leader_report | id=%s | "
                    "client_id=%s | deduper_id=%s",
                    ID,
                    client_id,
                    control.sender_id,
                )
                return

            self._leader_reports_by_client[client_id].add(control.sender_id)
            total = (
                self._leader_totals_by_client.get(client_id, 0)
                + control.expected_total
            )
            self._leader_totals_by_client[client_id] = total
            report_count = len(self._leader_reports_by_client[client_id])
            if report_count >= Q4_DEDUPER_AMOUNT:
                should_forward = True
                self._leader_reports_by_client.pop(client_id, None)
                self._leader_totals_by_client.pop(client_id, None)
                self._leader_closed_by_client.add(client_id)

        logging.info(
            "q4_deduper_leader_report | id=%s | client_id=%s | "
            "deduper_id=%s | report_count=%s | expected_reports=%s | "
            "emitted_accounts=%s",
            ID,
            client_id,
            control.sender_id,
            report_count,
            Q4_DEDUPER_AMOUNT,
            control.expected_total,
        )

        if should_forward:
            self._send_gateway_eof(client_id, total, output or self._gateway_eof_output)

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.DATA:
                self._accept_accounts(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_eof(client_id, payload)
            else:
                raise ValueError(
                    f"unexpected q4 account deduper message type: {msg_type}"
                )
            ack()
        except Exception:
            logging.exception("q4_deduper_error | id=%s", ID)
            nack()

    def _on_leader_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type != MessageType.EOF_RECEIVED:
                raise ValueError(
                    f"unexpected q4 account deduper leader message type: {msg_type}"
                )
            self._handle_leader_report(
                client_id,
                self._control_serializer.deserialize(payload),
                self._gateway_eof_output,
            )
            ack()
        except Exception:
            logging.exception("q4_deduper_leader_error | id=%s", ID)
            nack()

    def _start_response_consumer(self) -> None:
        if self._response_consumer is None:
            return
        self._response_thread = threading.Thread(
            target=self._response_consumer.start_consuming,
            args=(self._on_leader_message,),
            daemon=True,
        )
        self._response_thread.start()

    def start(self) -> None:
        logging.info(
            "q4_deduper_start | id=%s | input_exchange=%s | input_key=%s | "
            "aggregator_amount=%s | deduper_amount=%s | "
            "gateway_queue=%s",
            ID,
            Q4_DEDUPER_EXCHANGE,
            self._input_routing_key(),
            Q4_AGGREGATOR_AMOUNT,
            Q4_DEDUPER_AMOUNT,
            GATEWAY_Q4_QUEUE,
        )
        self._start_response_consumer()
        try:
            if not self._stopped:
                self._input.start_consuming(self._on_message)
        finally:
            self.handle_sigterm()
            self.close()

    def handle_sigterm(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        logging.info("q4_deduper_shutdown | id=%s", ID)
        self._input.request_stop_consuming()
        if self._response_consumer is not None:
            self._response_consumer.request_stop_consuming()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in (
            self._input,
            self._gateway_output,
            self._leader_output,
            self._response_consumer,
            self._gateway_eof_output,
        ):
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "q4_deduper_close_error | id=%s | error=%s",
                    ID,
                    e,
                )
        if self._response_thread is not None:
            self._response_thread.join(timeout=5)
