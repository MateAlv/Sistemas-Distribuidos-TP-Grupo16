import logging
import select
import socket
import threading
import queue
import os
from dataclasses import dataclass, field

from common.bank_ids import notebook_bank_id
from common.message_protocol.external import FileChunk, FileEof, recv_exact, sendall
from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
    HANDSHAKE, FILE_CHUNK, FINISH, ACK,
    MSG_CHUNK, MSG_EOF, MSG_ABORT,
    file_ingestor_routing_key,
    file_type_name,
)
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
    ensure_exchange_queue_bindings,
)
from common.message_protocol.internal import InternalProtocol, TransactionSerializer
from common.message_protocol.internal.common import MessageType
from common.message_protocol.internal.partial_result_serializer import Q2BankMaxResultSerializer
from common.message_protocol.internal.scatter_gather_serializer import Q4AccountIdSerializer
from common.message_protocol.internal.aggregation_serializer import AggregationSerializer


@dataclass(frozen=True)
class GatewayConfig:
    server_host: str
    server_port: int
    mom_host: str
    file_ingestor_exchange: str
    file_ingestor_partitions: int
    file_splitter_queue_prefix: str
    logging_level: str


@dataclass
class ClientSession:
    client_id: int
    sock: socket.socket
    chunks_forwarded: int = 0
    payload_bytes_forwarded: int = 0
    result_lines_sent: int = 0
    result_bytes_sent: int = 0
    used_partitions: set[int] = field(default_factory=set)
    current_file: tuple[str, int, int] | None = None


DEFAULT_CHUNK_LOG_EVERY = 100
DEFAULT_RESULT_LOG_EVERY = 100


class Gateway:
    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self._server_sock: socket.socket | None = None
        self._stopped = False

        self._client_queues = {}
        self._pending_eofs_by_client = {}  # client_id -> pending EOF count
        self._addressed_result_seen: set[tuple[str, int, int, int]] = set()
        self._client_queues_lock = threading.Lock()

        self._q1_queue_name = (
            os.environ.get("GATEWAY_QUEUE", "gateway_results_queue")
            if os.environ.get("GATEWAY_Q1_ENABLED", "1") != "0"
            else None
        )
        q2_queue = os.environ.get("GATEWAY_Q2_QUEUE")
        q3_queue = os.environ.get("GATEWAY_Q3_QUEUE")
        q4_queue = os.environ.get("GATEWAY_Q4_QUEUE")
        q5_queue = os.environ.get("GATEWAY_Q5_QUEUE")
        self._q2_queue_name = q2_queue
        self._q3_queue_name = q3_queue
        self._q4_queue_name = q4_queue
        self._q5_queue_name = q5_queue
        self._num_result_queues = (
            int(bool(self._q1_queue_name))
            + int(bool(q2_queue))
            + int(bool(self._q3_queue_name))
            + int(bool(q4_queue))
            + int(bool(q5_queue))
        )

        self._consumers: list[MessageMiddlewareQueueRabbitMQ] = []
        self._consumer_threads: list[threading.Thread] = []
        self._client_threads: list[threading.Thread] = []
        self._client_socks: set[socket.socket] = set()

    def run(self) -> None:
        self._ensure_file_splitter_bindings()

        self._start_result_consumer(self._q1_queue_name, self._q1_csv_lines, "")
        self._start_result_consumer(
            self._q2_queue_name, self._q2_csv_lines, "Q2|", addressed=True
        )
        self._start_result_consumer(self._q3_queue_name, self._q3_csv_lines, "Q3|")
        self._start_result_consumer(self._q4_queue_name, self._q4_csv_lines, "Q4|", addressed=True)
        self._start_result_consumer(self._q5_queue_name, self._q5_csv_lines, "Q5|", addressed=True)

        try:
            self._accept_clients()
        finally:
            self._shutdown_workers()

    def _start_result_consumer(
        self, queue_name, payload_to_csv_lines, prefix: str, addressed: bool = False
    ) -> None:
        if not queue_name:
            return
        consumer = MessageMiddlewareQueueRabbitMQ(self._config.mom_host, queue_name)
        self._consumers.append(consumer)
        thread = threading.Thread(
            target=self._run_result_consumer,
            args=(consumer, payload_to_csv_lines, prefix, addressed),
            daemon=True,
        )
        self._consumer_threads.append(thread)
        thread.start()

    def _accept_clients(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            self._server_sock = server_sock
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self._config.server_host, self._config.server_port))
            server_sock.listen()
            logging.info(
                "gateway_listen | host=%s | port=%s",
                self._config.server_host,
                self._config.server_port,
            )

            while not self._stopped:
                try:
                    client_sock, addr = server_sock.accept()
                except OSError:
                    if self._stopped:
                        break
                    raise

                logging.info("gateway_accept | addr=%s", addr)
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_sock,),
                    daemon=True,
                )
                with self._client_queues_lock:
                    self._client_threads = [
                        t for t in self._client_threads if t.is_alive()
                    ]
                    self._client_threads.append(thread)
                thread.start()

    def stop(self) -> None:
        """Signal shutdown; the joins and FD closes happen in _shutdown_workers."""
        if self._stopped:
            return
        self._stopped = True
        logging.info("gateway_stop")

        self._close_server_socket()

        for consumer in self._consumers:
            consumer.request_stop_consuming()

        self._shutdown_client_sockets()

    def _shutdown_workers(self) -> None:
        # stop() may not have run if the accept loop died on an error.
        if not self._stopped:
            self.stop()

        for thread in self._consumer_threads:
            thread.join(timeout=5)
            if thread.is_alive():
                logging.warning("gateway_consumer_join_timeout | thread=%s", thread.name)

        self._shutdown_client_sockets()
        with self._client_queues_lock:
            client_threads = list(self._client_threads)
        for thread in client_threads:
            thread.join(timeout=5)

        self._close_client_sockets()
        for thread in client_threads:
            thread.join(timeout=2)
            if thread.is_alive():
                logging.warning("gateway_client_join_timeout | thread=%s", thread.name)

        self._close_server_socket()

    def _close_server_socket(self) -> None:
        sock = self._server_sock
        if sock is None:
            return
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass

    def _shutdown_client_sockets(self) -> None:
        with self._client_queues_lock:
            socks = list(self._client_socks)
        for sock in socks:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _close_client_sockets(self) -> None:
        with self._client_queues_lock:
            socks = list(self._client_socks)
            self._client_socks.clear()
        for sock in socks:
            try:
                sock.close()
            except OSError:
                pass

    def _handle_client(self, client_sock: socket.socket) -> None:
        client_id = None
        with self._client_queues_lock:
            self._client_socks.add(client_sock)
        try:
            client_id = self._serve_client(client_sock)
        except Exception as e:
            if self._stopped:
                logging.info("gateway_client_shutdown | client_id=%s", client_id)
            else:
                logging.error(
                    "gateway_client_error | client_id=%s | error=%s", client_id, e
                )
        finally:
            with self._client_queues_lock:
                self._client_socks.discard(client_sock)
            try:
                client_sock.close()
            except OSError:
                pass
            if client_id is not None:
                logging.info("gateway_session_terminated | client_id=%s", client_id)

    def _serve_client(self, client_sock: socket.socket) -> int:
        client_id = self._recv_handshake(client_sock)
        session = ClientSession(client_id=client_id, sock=client_sock)

        q = queue.Queue()
        with self._client_queues_lock:
            self._client_queues[client_id] = q
            self._pending_eofs_by_client[client_id] = self._num_result_queues

        chunks_finished = False
        try:
            self._forward_chunks(session)
            chunks_finished = True
            self._wait_for_results(session, q)
        finally:
            with self._client_queues_lock:
                self._client_queues.pop(client_id, None)
                self._pending_eofs_by_client.pop(client_id, None)
                self._addressed_result_seen = {
                    key for key in self._addressed_result_seen if key[1] != client_id
                }
            if not chunks_finished:
                self._send_abort(session.client_id)

        return client_id

    def _send_abort(self, client_id: int) -> None:
        """Broadcast ABORT to every file-splitter partition so each worker can
        discard orphaned per-client state. Best-effort: if this send fails the
        state leaks until the next gateway restart, which is acceptable."""
        partitions = self._config.file_ingestor_partitions
        publisher = FileIngestorPublisher(
            mom_host=self._config.mom_host,
            exchange_name=self._config.file_ingestor_exchange,
        )
        try:
            abort_msg = _serialize_abort(client_id)
            for partition in range(partitions):
                publisher.send(partition, abort_msg)
            logging.info(
                "gateway_abort_sent | client_id=%s | partitions=%s",
                client_id, partitions,
            )
        except Exception as e:
            logging.error(
                "gateway_abort_error | client_id=%s | error=%s", client_id, e
            )
        finally:
            publisher.close()

    def _recv_handshake(self, client_sock: socket.socket) -> int:
        msg_type = int.from_bytes(recv_exact(client_sock, 1), "big")
        if msg_type != HANDSHAKE:
            raise RuntimeError(f"expected handshake, got msg_type={msg_type}")

        client_id = int.from_bytes(recv_exact(client_sock, 4), "big")
        _send_ack(client_sock)
        logging.info("gateway_handshake | client_id=%s", client_id)
        return client_id

    def _forward_chunks(self, session: ClientSession) -> None:
        cfg = self._config
        publisher = FileIngestorPublisher(
            mom_host=cfg.mom_host,
            exchange_name=cfg.file_ingestor_exchange,
        )

        try:
            while True:
                msg_type = int.from_bytes(recv_exact(session.sock, 1), "big")

                if msg_type == FILE_CHUNK:
                    chunk = FileChunk.recv(session.sock)
                    if chunk.client_id() != session.client_id:
                        raise RuntimeError(
                            "chunk client_id mismatch "
                            f"(handshake={session.client_id}, chunk={chunk.client_id()})"
                        )

                    partition = partition_for(
                        client_id=session.client_id,
                        file_type=chunk.file_type(),
                        partitions=cfg.file_ingestor_partitions,
                    )
                    current_file = (chunk.path(), chunk.file_type(), partition)
                    if session.current_file is None:
                        session.current_file = current_file
                    elif session.current_file[:2] != current_file[:2]:
                        prev_path, prev_file_type, prev_partition = session.current_file
                        publisher.send(
                            prev_partition,
                            _serialize_file_eof(
                                session.client_id,
                                prev_file_type,
                                prev_path,
                            ),
                        )
                        logging.debug(
                            "gateway_forward_file_eof | client_id=%s | path=%s | "
                            "file_type=%s | partition=%s | routing_key=%s",
                            session.client_id,
                            prev_path,
                            prev_file_type,
                            prev_partition,
                            file_ingestor_routing_key(prev_partition),
                        )
                        logging.info(
                            "gateway_file_eof_sent | client_id=%s | path=%s | "
                            "file_type=%s | partition=%s | routing_key=%s | "
                            "reason=file_switch",
                            session.client_id,
                            prev_path,
                            file_type_name(prev_file_type),
                            prev_partition,
                            file_ingestor_routing_key(prev_partition),
                        )
                        session.current_file = current_file

                    publisher.send(partition, _serialize_chunk(chunk))
                    session.chunks_forwarded += 1
                    session.payload_bytes_forwarded += chunk.payload_size()
                    session.used_partitions.add(partition)
                    logging.debug(
                        "gateway_forward_chunk | client_id=%s | path=%s | "
                        "offset=%s | partition=%s | routing_key=%s",
                        session.client_id,
                        chunk.path(),
                        chunk.offset(),
                        partition,
                        file_ingestor_routing_key(partition),
                    )
                    if _should_log_progress(
                        session.chunks_forwarded,
                        _env_int("CHUNK_LOG_EVERY", DEFAULT_CHUNK_LOG_EVERY),
                    ):
                        logging.info(
                            "gateway_chunk_forwarded | client_id=%s | chunks=%s | "
                            "payload_bytes=%s | file_type=%s | path=%s | "
                            "offset=%s | last_payload_bytes=%s | partition=%s | "
                            "routing_key=%s",
                            session.client_id,
                            session.chunks_forwarded,
                            session.payload_bytes_forwarded,
                            file_type_name(chunk.file_type()),
                            chunk.path(),
                            chunk.offset(),
                            chunk.payload_size(),
                            partition,
                            file_ingestor_routing_key(partition),
                        )
                    _send_ack(session.sock)

                elif msg_type == FINISH:
                    if session.current_file is not None:
                        path, file_type, partition = session.current_file
                        publisher.send(
                            partition,
                            _serialize_file_eof(session.client_id, file_type, path),
                        )
                        logging.debug(
                            "gateway_forward_file_eof | client_id=%s | path=%s | "
                            "file_type=%s | partition=%s | routing_key=%s",
                            session.client_id,
                            path,
                            file_type,
                            partition,
                            file_ingestor_routing_key(partition),
                        )
                        logging.info(
                            "gateway_file_eof_sent | client_id=%s | path=%s | "
                            "file_type=%s | partition=%s | routing_key=%s | "
                            "reason=client_finish",
                            session.client_id,
                            path,
                            file_type_name(file_type),
                            partition,
                            file_ingestor_routing_key(partition),
                        )
                        session.current_file = None

                    _send_ack(session.sock)
                    logging.info(
                        "gateway_finish | client_id=%s | chunks=%s | "
                        "payload_bytes=%s | partitions=%s",
                        session.client_id,
                        session.chunks_forwarded,
                        session.payload_bytes_forwarded,
                        sorted(session.used_partitions),
                    )
                    break

                else:
                    raise RuntimeError(
                        f"unexpected msg_type={msg_type} from client_id={session.client_id}"
                    )
        finally:
            publisher.close()

    def _wait_for_results(self, session: ClientSession, q: queue.Queue) -> None:
        logging.info("gateway_results_wait | client_id=%s", session.client_id)
        
        while not self._stopped:
            try:
                result = q.get(timeout=1.0)
                if result is None:
                    logging.info("gateway_results_eof | client_id=%s", session.client_id)
                    break
                encoded = result.encode("utf-8")
                sendall(session.sock, encoded)
                session.result_lines_sent += 1
                session.result_bytes_sent += len(encoded)
                if _should_log_progress(
                    session.result_lines_sent,
                    _env_int("RESULT_LOG_EVERY", DEFAULT_RESULT_LOG_EVERY),
                ):
                    logging.info(
                        "gateway_result_sent | client_id=%s | lines=%s | bytes=%s",
                        session.client_id,
                        session.result_lines_sent,
                        session.result_bytes_sent,
                    )
            except queue.Empty:
                readable, _, _ = select.select([session.sock], [], [], 0.0)
                if readable:
                    data = session.sock.recv(1, socket.MSG_PEEK)
                    if not data:
                        logging.info("gateway_client_closed | client_id=%s", session.client_id)
                        return  

    def _run_result_consumer(
        self, consumer, payload_to_csv_lines, prefix: str, addressed: bool = False
    ) -> None:
        internal_serializer = InternalProtocol()

        def callback(message, ack, nack):
            try:
                if addressed:
                    msg_type, client_id, sender_id, seq, payload = (
                        internal_serializer.unpack_addressed_packet(message)
                    )
                else:
                    msg_type, client_id, payload = internal_serializer.unpack_packet(message)

                with self._client_queues_lock:
                    client_queue = self._client_queues.get(client_id)
                    if not client_queue:
                        ack()
                        return

                    if addressed:
                        dedup_key = (prefix, client_id, sender_id, seq)
                        if dedup_key in self._addressed_result_seen:
                            ack()
                            return
                        self._addressed_result_seen.add(dedup_key)

                    if msg_type == MessageType.EOF:
                        remaining = self._pending_eofs_by_client.get(client_id, 1) - 1
                        self._pending_eofs_by_client[client_id] = remaining
                        should_close = remaining <= 0
                    else:
                        should_close = False

                if msg_type == MessageType.DATA:
                    for csv_line in payload_to_csv_lines(payload):
                        client_queue.put(prefix + csv_line + "\n")
                elif msg_type == MessageType.EOF:
                    if should_close:
                        client_queue.put(None)
                    logging.info(
                        "gateway_eof | prefix=%s | client_id=%s | remaining=%s",
                        prefix or "Q1", client_id, 0 if should_close else "pending",
                    )

                ack()
            except Exception as e:
                logging.error("gateway_results_consumer_error | prefix=%s | error=%s", prefix or "Q1", e)
                nack()

        try:
            consumer.start_consuming(callback)
        except Exception as e:
            if not self._stopped:
                logging.error("gateway_results_consumer_stopped | prefix=%s | error=%s", prefix or "Q1", e)
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    def _ensure_file_splitter_bindings(self) -> None:
        bindings = file_splitter_bindings(
            self._config.file_splitter_queue_prefix,
            self._config.file_ingestor_partitions,
        )
        ensure_exchange_queue_bindings(
            self._config.mom_host,
            self._config.file_ingestor_exchange,
            bindings,
        )
        logging.info(
            "gateway_file_splitter_bindings_ready | exchange=%s | partitions=%s",
            self._config.file_ingestor_exchange,
            self._config.file_ingestor_partitions,
        )

    @staticmethod
    def _q1_csv_lines(payload: bytes):
        for tx in TransactionSerializer().deserialize_batch(payload):
            yield (
                f"{tx.from_bank},{tx.from_account},"
                f"{tx.to_bank},{tx.to_account},{tx.amount:.2f}"
            )

    @staticmethod
    def _q2_csv_lines(payload: bytes):
        yield Gateway._q2_csv(payload)

    @staticmethod
    def _q2_csv(payload: bytes) -> str:
        result = Q2BankMaxResultSerializer.deserialize(payload)
        return (
            f"{result.bank_id},{result.from_account},"
            f"{result.bank_name},{result.amount:.2f}"
        )

    @staticmethod
    def _q3_csv_lines(payload: bytes):
        for tx in TransactionSerializer().deserialize_batch(payload):
            yield (
                f"{notebook_bank_id(tx.from_bank)},"
                f"{tx.from_account},{tx.format},{tx.amount:.2f}"
            )

    @staticmethod
    def _q4_csv_lines(payload: bytes):
        for account in Q4AccountIdSerializer.deserialize_batch(payload):
            yield f"{account.bank_id},{account.account}"

    @staticmethod
    def _q4_csv(payload: bytes) -> str:
        account = Q4AccountIdSerializer.deserialize(payload)
        return f"{account.bank_id},{account.account}"

    @staticmethod
    def _q5_csv_lines(payload: bytes):
        yield Gateway._q5_csv(payload)

    @staticmethod
    def _q5_csv(payload: bytes) -> str:
        count = AggregationSerializer.deserialize(payload)
        return str(count)


def _send_ack(sock: socket.socket) -> None:
    sendall(sock, ACK.to_bytes(1, "big"))


def _serialize_chunk(chunk: FileChunk) -> bytes:
    return MSG_CHUNK.to_bytes(1, "big") + chunk.serialize()


def _serialize_abort(client_id: int) -> bytes:
    return MSG_ABORT.to_bytes(1, "big") + client_id.to_bytes(4, "big")


def _serialize_file_eof(client_id: int, file_type: int, rel_path: str) -> bytes:
    return MSG_EOF.to_bytes(1, "big") + FileEof(
        rel_path=rel_path,
        client_id=client_id,
        file_type=file_type,
    ).serialize()


def _should_log_progress(count: int, every: int) -> bool:
    return count == 1 or (every > 0 and count % every == 0)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


class FileIngestorPublisher:
    def __init__(self, mom_host: str, exchange_name: str) -> None:
        self._mom_host = mom_host
        self._exchange_name = exchange_name
        self._senders: dict[int, MessageMiddlewareExchangeRabbitMQ] = {}

    def send(self, partition: int, message: bytes) -> None:
        self._sender_for(partition).send(message)

    def close(self) -> None:
        for partition, sender in self._senders.items():
            try:
                sender.close()
            except Exception as e:
                logging.warning(
                    "gateway_publisher_close_error | partition=%s | error=%s",
                    partition,
                    e,
                )
        self._senders.clear()

    def _sender_for(self, partition: int) -> MessageMiddlewareExchangeRabbitMQ:
        if partition not in self._senders:
            self._senders[partition] = MessageMiddlewareExchangeRabbitMQ(
                host=self._mom_host,
                exchange_name=self._exchange_name,
                routing_keys=[file_ingestor_routing_key(partition)],
            )
        return self._senders[partition]


def partition_for(client_id: int, file_type: int, partitions: int) -> int:
    if partitions <= 0:
        raise ValueError("partitions must be greater than 0")

    base_partition = (int(client_id) * 2) % partitions
    if file_type == FILE_TYPE_TRANSACTIONS:
        return base_partition
    if file_type == FILE_TYPE_ACCOUNTS:
        return (base_partition + 1) % partitions
    raise ValueError(f"unknown file_type for partitioning: {file_type}")


def file_splitter_bindings(queue_prefix: str, partitions: int) -> dict[str, str]:
    if partitions <= 0:
        raise ValueError("partitions must be greater than 0")
    return {
        f"{queue_prefix}_{partition}": file_ingestor_routing_key(partition)
        for partition in range(partitions)
    }
