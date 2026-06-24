import logging
import os
import threading

from common.fault_tolerance.handler import Action, PersistentStateHandler
from common.fault_tolerance.inbox import InboxStatus, MsgKind
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
from common.routing import queue_name_for_worker

try:
    from q4_deduper_state import Q4DeduperState
except ImportError:
    from workers.scatter_gather.q4_deduper.q4_deduper_state import Q4DeduperState


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
Q4_DEDUPER_BATCH_MAX_ACCOUNTS = int(
    os.environ.get("Q4_DEDUPER_BATCH_MAX_ACCOUNTS", "5000")
)
STATE_DIR = os.environ.get("STATE_DIR", "")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))
GATEWAY_Q4_DATA_EDGE = "gateway_q4_data"
GATEWAY_Q4_EOF_EDGE = "gateway_q4_eof"
Q4_DEDUPER_LEADER_EDGE = "q4_deduper_leader"
LEADER_REPORT_KIND = MsgKind.CTRL_FLUSH_ACK
GATEWAY_EOF_SEQ = 0xFFFFFFFF


class Q4DeduperWorker:
    """Deduplicates notebook Q4 account candidates and writes gateway batches."""

    def __init__(self):
        self._proto = InternalProtocol()
        self._account_serializer = Q4AccountIdSerializer()
        self._control_serializer = ControlMessageSerializer()

        self._lock = threading.Lock()
        self._state = Q4DeduperState(Q4_AGGREGATOR_AMOUNT, Q4_DEDUPER_AMOUNT)
        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"q4_deduper_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
        )

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
        self._publishers = {
            GATEWAY_Q4_DATA_EDGE: self._gateway_output,
            GATEWAY_Q4_EOF_EDGE: self._gateway_eof_output or self._gateway_output,
            Q4_DEDUPER_LEADER_EDGE: self._leader_output,
        }

        self._response_thread = None
        self._closed = False
        self._stopped = False

        self._handler.recover()
        self._republish_pending()

    def _input_routing_key(self) -> str:
        return queue_name_for_worker(Q4_DEDUPER_ROUTING_PREFIX, ID)

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

    def _gateway_packet(
        self, msg_type: MessageType, client_id: int, seq: int, payload: bytes
    ) -> bytes:
        return self._proto.create_addressed_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            sender_id=ID,
            seq=seq,
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

    def _iter_unique_accounts(self, keys: set[tuple[str, str]]):
        for key in sorted(keys):
            yield self._account_from_key(key)

    def _build_gateway_data_outputs(self, client_id: int) -> list:
        outputs = []
        keys = self._state.accounts_for(client_id)
        accounts = list(self._iter_unique_accounts(keys))
        for seq, chunk in enumerate(_chunks(accounts, Q4_DEDUPER_BATCH_MAX_ACCOUNTS)):
            payload = self._account_serializer.serialize_batch(chunk)
            outputs.append(
                (
                    GATEWAY_Q4_DATA_EDGE,
                    self._gateway_packet(MessageType.DATA, client_id, seq, payload),
                )
            )
        return outputs

    def _build_gateway_eof_output(self, client_id: int, expected_total: int) -> tuple:
        logging.info(
            "q4_deduper_forward_gateway_eof | id=%s | client_id=%s | "
            "expected_total=%s",
            ID,
            client_id,
            expected_total,
        )
        return (
            GATEWAY_Q4_EOF_EDGE,
            self._gateway_packet(
                MessageType.EOF,
                client_id,
                GATEWAY_EOF_SEQ,
                self._control_payload(ID, expected_total, 0),
            ),
        )

    def _build_leader_report_output(self, client_id: int, emitted_accounts: int) -> tuple:
        if self._leader_output is None:
            raise RuntimeError("leader output is not configured")
        logging.info(
            "q4_deduper_report_leader | id=%s | client_id=%s | "
            "emitted_accounts=%s",
            ID,
            client_id,
            emitted_accounts,
        )
        return (
            Q4_DEDUPER_LEADER_EDGE,
            self._packet(
                MessageType.EOF_RECEIVED,
                client_id,
                self._control_payload(ID, emitted_accounts, 0),
            ),
        )

    def _build_close_outputs(self, client_id: int) -> tuple[list, int]:
        emitted_accounts = len(self._state.accounts_for(client_id))
        outputs = self._build_gateway_data_outputs(client_id)
        if Q4_DEDUPER_AMOUNT == 1:
            outputs.append(self._build_gateway_eof_output(client_id, emitted_accounts))
        else:
            outputs.append(self._build_leader_report_output(client_id, emitted_accounts))
        return outputs, emitted_accounts

    def _publish(self, entries) -> None:
        for entry in entries:
            publisher = self._publishers.get(entry.destination)
            if publisher is None:
                raise KeyError(f"no publisher for destination {entry.destination!r}")
            publisher.send(entry.body)

    def _publish_then_commit(self, instruction) -> None:
        if instruction.action is Action.PUBLISH_THEN_COMMIT:
            self._publish(instruction.outputs)
            with self._lock:
                self._handler.commit_done(*instruction.ctx)

    def _republish_pending(self) -> None:
        for entry in self._handler.outbox_to_republish():
            try:
                self._publish([entry])
            except Exception:
                logging.exception(
                    "q4_deduper_republish_error | id=%s | destination=%s",
                    ID,
                    entry.destination,
                )

    # ---------- data path ----------

    def _handle_data(self, msg_id, client_id, sender_id, seq, payload, ack) -> None:
        def bfn(data):
            return Q4DeduperState.data_change(client_id, data), []

        with self._lock:
            status = self._handler.inbox.classify(client_id, sender_id, seq)
            if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                ack()
                return
            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, payload, bfn
            )
        self._publish_then_commit(instruction)
        ack()

        processed = self._state.processed_count(client_id)
        if should_log_progress(processed):
            logging.info(
                "q4_deduper_data_batch | id=%s | client_id=%s | "
                "processed_total=%s | in_memory_unique=%s",
                ID,
                client_id,
                processed,
                len(self._state.accounts_for(client_id)),
            )

    def _handle_eof(self, msg_id, client_id, sender_id, seq, payload, ack) -> None:
        control = self._control_serializer.deserialize(payload)
        upstream_id = control.sender_id

        with self._lock:
            status = self._handler.inbox.classify(client_id, sender_id, seq)
            if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                ack()
                return
            completes = self._state.eof_would_complete(client_id, upstream_id)
            eof_count = self._state.eof_count(client_id)
            processed_total = self._state.processed_count(client_id)

            def bfn(_data):
                eof_change = Q4DeduperState.eof_change(client_id, upstream_id)
                if completes:
                    outputs, _emitted = self._build_close_outputs(client_id)
                    return (
                        Q4DeduperState.compound_change(
                            eof_change,
                            Q4DeduperState.close_change(client_id),
                        ),
                        outputs,
                    )
                return eof_change, []

            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, payload, bfn
            )

        self._publish_then_commit(instruction)
        ack()
        logging.info(
            "q4_deduper_eof_received | id=%s | client_id=%s | "
            "pair_reducer_id=%s | eof_count=%s | expected_eofs=%s | "
            "sender_expected_total=%s | processed_total=%s | flushed=%s",
            ID,
            client_id,
            upstream_id,
            eof_count + 1,
            Q4_AGGREGATOR_AMOUNT,
            control.expected_total,
            processed_total,
            completes,
        )

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self._proto.unpack_addressed_packet(raw)
            )
            msg_id = f"{sender_id}:{seq}"
            if msg_type == MessageType.DATA:
                self._handle_data(msg_id, client_id, sender_id, seq, payload, ack)
            elif msg_type == MessageType.EOF:
                self._handle_eof(msg_id, client_id, sender_id, seq, payload, ack)
            else:
                raise ValueError(
                    f"unexpected q4 account deduper message type: {msg_type}"
                )
        except Exception:
            logging.exception("q4_deduper_error | id=%s", ID)
            nack(requeue=True)

    # ---------- leader report path ----------

    def _handle_leader_report(
        self,
        client_id: int,
        control: ControlMessage,
        raw: bytes,
        ack,
    ) -> None:
        if ID != 0:
            ack()
            return

        sender_id = control.sender_id
        seq = client_id
        msg_id = f"leader:{sender_id}:{client_id}"

        with self._lock:
            status = self._handler.inbox.classify(
                client_id, sender_id, seq, LEADER_REPORT_KIND
            )
            if self._state.is_leader_closed(client_id) and status is not InboxStatus.APPLIED:
                ack()
                return
            completes = self._state.leader_report_would_complete(client_id, sender_id)
            report_count = self._state.leader_report_count(client_id)
            total_after = self._state.leader_total(client_id) + control.expected_total

            def bfn(_data):
                report_change = Q4DeduperState.leader_report_change(
                    client_id,
                    sender_id,
                    control.expected_total,
                )
                if completes:
                    return (
                        Q4DeduperState.compound_change(
                            report_change,
                            Q4DeduperState.leader_close_change(client_id),
                        ),
                        [self._build_gateway_eof_output(client_id, total_after)],
                    )
                return report_change, []

            instruction = self._handler.handle(
                msg_id,
                client_id,
                sender_id,
                seq,
                raw,
                bfn,
                kind=LEADER_REPORT_KIND,
            )

        self._publish_then_commit(instruction)
        ack()
        logging.info(
            "q4_deduper_leader_report | id=%s | client_id=%s | "
            "deduper_id=%s | report_count=%s | expected_reports=%s | "
            "emitted_accounts=%s | flushed=%s",
            ID,
            client_id,
            sender_id,
            report_count + 1,
            Q4_DEDUPER_AMOUNT,
            control.expected_total,
            completes,
        )

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
                raw,
                ack,
            )
        except Exception:
            logging.exception("q4_deduper_leader_error | id=%s", ID)
            nack(requeue=True)

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
            "aggregator_amount=%s | deduper_amount=%s | gateway_queue=%s",
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


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]
