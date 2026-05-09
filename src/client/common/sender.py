import logging
import socket
import time

from common.socket_utils import ensure_socket, recv_exact, sendall


H_ID_HANDSHAKE = 1
H_ID_DATA = 2
H_ID_FINISH = 3
H_ID_ACK = 4


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

    def __enter__(self) -> "Sender":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def send_handshake_request(self, client_id: int) -> None:
        ensure_socket(self._sock)

        payload = _header_id_to_bytes(H_ID_HANDSHAKE)
        payload += int(client_id).to_bytes(4, byteorder="big")
        sendall(self._sock, payload)
        self._wait_ack("handshake")

    def send_file_chunk(self, data: bytes) -> None:
        ensure_socket(self._sock)

        if not data:
            return

        sendall(self._sock, _header_id_to_bytes(H_ID_DATA) + data)
        self._wait_ack("file chunk")

    def send_finished(self) -> None:
        ensure_socket(self._sock)

        sendall(self._sock, _header_id_to_bytes(H_ID_FINISH))
        self._wait_ack("finish")

    def _wait_ack(self, operation: str) -> None:
        ensure_socket(self._sock)

        header = _header_id_from_bytes(recv_exact(self._sock, 1))
        if header != H_ID_ACK:
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
