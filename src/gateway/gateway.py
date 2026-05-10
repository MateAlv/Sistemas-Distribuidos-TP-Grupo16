import logging
import socket
import threading
from dataclasses import dataclass

from common.message_protocol.external import FileChunk, recv_exact, sendall
from common.message_protocol.external.types import (
    HANDSHAKE, FILE_CHUNK, FINISH, ACK,
    MSG_CHUNK, MSG_EOF,
    RES_RESULT, RES_EOF,
)
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ


@dataclass(frozen=True)
class GatewayConfig:
    server_host: str
    server_port: int
    mom_host: str
    chunks_queue: str
    results_queue: str
    logging_level: str


class Gateway:
    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self._server_sock: socket.socket | None = None
        self._stopped = False

    def run(self) -> None:
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
        if self._server_sock is not None:
            try:
                self._server_sock.shutdown(socket.SHUT_RDWR)
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
        self._forward_chunks(client_sock, client_id)
        self._stream_results(client_sock, client_id)

    def _recv_handshake(self, client_sock: socket.socket) -> int:
        msg_type = int.from_bytes(recv_exact(client_sock, 1), "big")
        if msg_type != HANDSHAKE:
            raise RuntimeError(f"expected handshake, got msg_type={msg_type}")

        client_id = int.from_bytes(recv_exact(client_sock, 4), "big")
        _send_ack(client_sock)
        logging.info("gateway_handshake | client_id=%s", client_id)
        return client_id

    def _forward_chunks(self, client_sock: socket.socket, client_id: int) -> None:
        chunks_forwarded = 0
        cfg = self._config

        with MessageMiddlewareQueueRabbitMQ(cfg.mom_host, cfg.chunks_queue) as queue:
            while True:
                msg_type = int.from_bytes(recv_exact(client_sock, 1), "big")

                if msg_type == FILE_CHUNK:
                    chunk = FileChunk.recv(client_sock)
                    queue.send(_serialize_chunk(chunk))
                    chunks_forwarded += 1
                    logging.debug(
                        "gateway_forward_chunk | client_id=%s | path=%s | offset=%s",
                        client_id,
                        chunk.path(),
                        chunk.offset(),
                    )
                    _send_ack(client_sock)

                elif msg_type == FINISH:
                    queue.send(_serialize_eof(client_id))
                    _send_ack(client_sock)
                    logging.info(
                        "gateway_finish | client_id=%s | chunks=%s",
                        client_id,
                        chunks_forwarded,
                    )
                    break

                else:
                    raise RuntimeError(
                        f"unexpected msg_type={msg_type} from client_id={client_id}"
                    )

    def _stream_results(self, client_sock: socket.socket, client_id: int) -> None:
        logging.info("gateway_results_start | client_id=%s", client_id)
        lines_sent = 0
        cfg = self._config

        with MessageMiddlewareQueueRabbitMQ(cfg.mom_host, cfg.results_queue) as queue:
            def on_result(message: bytes, ack, nack) -> None:
                nonlocal lines_sent
                try:
                    msg_type = message[0]
                    payload = message[1:]

                    if msg_type == RES_RESULT:
                        line = payload.decode("utf-8")
                        sendall(client_sock, (line + "\n").encode("utf-8"))
                        lines_sent += 1
                        ack()

                    elif msg_type == RES_EOF:
                        ack()
                        queue.stop_consuming()

                    else:
                        logging.warning("gateway_unknown_result | msg_type=%s", msg_type)
                        nack()

                except Exception as e:
                    logging.error("gateway_result_send_error | error=%s", e)
                    nack()

            queue.start_consuming(on_result)

        logging.info(
            "gateway_results_done | client_id=%s | lines=%s",
            client_id,
            lines_sent,
        )


def _send_ack(sock: socket.socket) -> None:
    sendall(sock, ACK.to_bytes(1, "big"))


def _serialize_chunk(chunk: FileChunk) -> bytes:
    # Wire layout: msg_type(1) | FileChunk bytes
    return MSG_CHUNK.to_bytes(1, "big") + chunk.serialize()


def _serialize_eof(client_id: int) -> bytes:
    # Wire layout: msg_type(1) | client_id(4)
    return MSG_EOF.to_bytes(1, "big") + client_id.to_bytes(4, "big")
