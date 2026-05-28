import logging
import socket
import time
from collections.abc import Generator

from common.message_protocol.external import ensure_socket, recv_exact, sendall
from common.message_protocol.external.types import HANDSHAKE, FILE_CHUNK, FINISH, ACK


RESULT_RECV_SIZE = 4096


class Sender:
    def __init__(
        self,
        host: str,
        port: int,
        connect_timeout: float = 10.0,
        io_timeout: float = 120.0,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.connect_timeout = float(connect_timeout)
        self.io_timeout = float(io_timeout)
        self._sock: socket.socket | None = None

    def connect(self) -> None:
        if self._sock is not None:
            return

        deadline = time.monotonic() + self.connect_timeout
        last_error = None

        while time.monotonic() < deadline:
            try:
                self._sock = socket.create_connection(
                    (self.host, self.port),
                    timeout=self.connect_timeout,
                )
                self._sock.settimeout(self.io_timeout)
                return
            except OSError as exc:
                last_error = exc
                time.sleep(0.2)

        raise OSError(f"could not connect to {self.host}:{self.port}") from last_error

    def close(self) -> None:
        if self._sock is None:
            return

        try:
            self._sock.close()
        finally:
            self._sock = None

    def send_handshake_request(self, client_id: int) -> None:
        ensure_socket(self._sock)

        payload = _header_id_to_bytes(HANDSHAKE)
        payload += int(client_id).to_bytes(4, byteorder="big")
        sendall(self._sock, payload)
        self._wait_ack("handshake")

    def send_file_chunk(self, data: bytes) -> None:
        ensure_socket(self._sock)

        if not data:
            return

        sendall(self._sock, _header_id_to_bytes(FILE_CHUNK) + data)
        self._wait_ack("file chunk")

    def send_finished(self) -> None:
        ensure_socket(self._sock)

        sendall(self._sock, _header_id_to_bytes(FINISH))
        self._wait_ack("finish")

    def iter_result_lines(
        self,
        max_line_size: int,
        encoding: str = "utf-8",
    ) -> Generator[str, None, None]:
        ensure_socket(self._sock)

        if max_line_size <= 0:
            raise ValueError("max_line_size must be greater than 0")

        line = bytearray()

        while True:
            data = self._sock.recv(RESULT_RECV_SIZE)
            if not data:
                if line:
                    yield _decode_result_line(line, encoding)
                return

            offset = 0
            while True:
                newline = data.find(b"\n", offset)
                if newline == -1:
                    line.extend(data[offset:])
                    _validate_result_line_size(line, max_line_size)
                    break

                line.extend(data[offset : newline + 1])
                _validate_result_line_size(line, max_line_size)
                yield _decode_result_line(line, encoding)
                line.clear()
                offset = newline + 1

    def _wait_ack(self, operation: str) -> None:
        ensure_socket(self._sock)

        header = _header_id_from_bytes(recv_exact(self._sock, 1))
        if header != ACK:
            raise RuntimeError(f"invalid ACK for {operation}: received {header}")
        logging.debug("ack received for %s", operation)


def _header_id_to_bytes(header: int) -> bytes:
    if not isinstance(header, int):
        raise TypeError(f"header must be int, got {type(header).__name__}")
    if not 0 <= header <= 255:
        raise ValueError(f"header out of range [0, 255]: {header}")
    return header.to_bytes(1, byteorder="big")


def _header_id_from_bytes(data: bytes) -> int:
    if len(data) != 1:
        raise ValueError(f"header must be exactly 1 byte, got {len(data)}")
    return int.from_bytes(data, byteorder="big")


def _validate_result_line_size(line: bytearray, max_line_size: int) -> None:
    if len(line) > max_line_size:
        raise ValueError(f"result line exceeded max_line_size={max_line_size}")


def _decode_result_line(line: bytearray, encoding: str) -> str:
    return bytes(line).decode(encoding, errors="replace").rstrip("\r\n")
