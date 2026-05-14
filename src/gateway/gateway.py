import hashlib
import logging
import select
import socket
import threading
import queue
import os
from dataclasses import dataclass, field

from common.message_protocol.external import FileChunk, recv_exact, sendall
from common.message_protocol.external.types import (
    HANDSHAKE, FILE_CHUNK, FINISH, ACK,
    MSG_CHUNK, MSG_EOF,
    file_ingestor_routing_key,
)
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.message_protocol.internal import InternalProtocol, TransactionSerializer
from common.message_protocol.common import MessageType


@dataclass(frozen=True)
class GatewayConfig:
    server_host: str
    server_port: int
    mom_host: str
    file_ingestor_exchange: str
    file_ingestor_partitions: int
    logging_level: str


@dataclass
class ClientSession:
    client_id: int
    sock: socket.socket
    chunks_forwarded: int = 0
    used_partitions: set[int] = field(default_factory=set)


class Gateway:
    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self._server_sock: socket.socket | None = None
        self._stopped = False
        
        self._client_queues = {}
        self._client_queues_lock = threading.Lock()
        self._results_consumer = None
        self._results_thread = None

    def run(self) -> None:
        gateway_queue = os.environ.get("GATEWAY_QUEUE", "gateway_results_queue")
        self._results_consumer = MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host, gateway_queue
        )
        self._results_thread = threading.Thread(target=self._consume_results_loop, daemon=True)
        self._results_thread.start()

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
                threading.Thread(
                    target=self._handle_client,
                    args=(client_sock,),
                    daemon=True,
                ).start()

    def stop(self) -> None:
        self._stopped = True
        if self._results_consumer is not None:
            try:
                self._results_consumer.stop_consuming()
                self._results_consumer.close()
            except Exception:
                pass
        if self._server_sock is not None:
            try:
                self._server_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._server_sock.close()
            except OSError:
                pass

    def _handle_client(self, client_sock: socket.socket) -> None:
        try:
            self._serve_client(client_sock)
        except Exception as e:
            logging.error("gateway_client_error | error=%s", e)
        finally:
            try:
                client_sock.close()
            except OSError:
                pass

    def _serve_client(self, client_sock: socket.socket) -> None:
        client_id = self._recv_handshake(client_sock)
        session = ClientSession(client_id=client_id, sock=client_sock)
        self._forward_chunks(session)
        self._wait_for_results(session)

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
                        rel_path=chunk.path(),
                        partitions=cfg.file_ingestor_partitions,
                    )
                    publisher.send(partition, _serialize_chunk(chunk))
                    session.chunks_forwarded += 1
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
                    _send_ack(session.sock)

                elif msg_type == FINISH:
                    for partition in sorted(session.used_partitions):
                        publisher.send(partition, _serialize_eof(session.client_id))
                        logging.debug(
                            "gateway_forward_eof | client_id=%s | partition=%s | "
                            "routing_key=%s",
                            session.client_id,
                            partition,
                            file_ingestor_routing_key(partition),
                        )

                    _send_ack(session.sock)
                    logging.info(
                        "gateway_finish | client_id=%s | chunks=%s | partitions=%s",
                        session.client_id,
                        session.chunks_forwarded,
                        sorted(session.used_partitions),
                    )
                    break

                else:
                    raise RuntimeError(
                        f"unexpected msg_type={msg_type} from client_id={session.client_id}"
                    )
        finally:
            publisher.close()

    def _wait_for_results(self, session: ClientSession) -> None:
        logging.info("gateway_results_wait | client_id=%s", session.client_id)
        
        q = queue.Queue()
        with self._client_queues_lock:
            self._client_queues[session.client_id] = q

        try:
            while not self._stopped:
                try:
                    result = q.get(timeout=1.0)
                    if result is None:
                        logging.info("gateway_results_eof | client_id=%s", session.client_id)
                        break
                    sendall(session.sock, result.encode("utf-8"))
                except queue.Empty:
                    readable, _, _ = select.select([session.sock], [], [], 0.0)
                    if readable:
                        data = session.sock.recv(1, socket.MSG_PEEK)
                        if not data:
                            logging.info("gateway_client_closed | client_id=%s", session.client_id)
                            return
        finally:
            with self._client_queues_lock:
                self._client_queues.pop(session.client_id, None)

    def _consume_results_loop(self) -> None:
        internal_serializer = InternalProtocol()
        transaction_serializer = TransactionSerializer()
        
        def callback(message, ack, nack):
            try:
                msg_type, client_id, payload = internal_serializer.unpack_packet(message)
                
                with self._client_queues_lock:
                    q = self._client_queues.get(client_id)
                
                if not q:
                    # Client disconnected or invalid ID, ignore message
                    ack()
                    return
                
                if msg_type == MessageType.DATA:
                    tx = transaction_serializer.deserialize(payload)
                    csv_line = f"{tx.date},{tx.from_bank},{tx.from_account},{tx.to_bank},{tx.to_account},{tx.amount},{tx.currency},{tx.format}\n"
                    q.put(csv_line)
                elif msg_type == MessageType.EOF:
                    q.put(None)
                
                ack()
            except Exception as e:
                logging.error(f"gateway_results_consumer_error | error={e}")
                nack()

        try:
            self._results_consumer.start_consuming(callback)
        except Exception as e:
            if not self._stopped:
                logging.error(f"gateway_results_consumer_stopped | error={e}")


def _send_ack(sock: socket.socket) -> None:
    sendall(sock, ACK.to_bytes(1, "big"))


def _serialize_chunk(chunk: FileChunk) -> bytes:
    # Wire layout: msg_type(1) | FileChunk bytes
    return MSG_CHUNK.to_bytes(1, "big") + chunk.serialize()


def _serialize_eof(client_id: int) -> bytes:
    # Wire layout: msg_type(1) | client_id(4)
    return MSG_EOF.to_bytes(1, "big") + client_id.to_bytes(4, "big")


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


def partition_for(client_id: int, rel_path: str, partitions: int) -> int:
    if partitions <= 0:
        raise ValueError("partitions must be greater than 0")

    key = f"{int(client_id)}:{rel_path}".encode("utf-8")
    return int(hashlib.md5(key).hexdigest(), 16) % partitions
