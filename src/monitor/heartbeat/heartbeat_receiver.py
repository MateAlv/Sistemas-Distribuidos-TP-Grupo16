import logging
import socket
import threading
import time
from collections.abc import Callable


DEFAULT_MONITOR_HOST = "0.0.0.0"
DEFAULT_MONITOR_PORT = 9000
MAX_HEARTBEAT_BYTES = 1024
RECEIVE_TIMEOUT_SECONDS = 0.5

Clock = Callable[[], float]


class HeartbeatReceiver:
    def __init__(
        self,
        host: str = DEFAULT_MONITOR_HOST,
        port: int = DEFAULT_MONITOR_PORT,
        clock: Clock = time.time,
    ) -> None:
        self._host = host
        self._port = port
        self._clock = clock

        self._last_seen: dict[str, float] = {}
        self._last_seen_lock = threading.Lock()
        self._socket_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._socket: socket.socket | None = None

    def listen(self) -> None:
        if self._stop_event.is_set():
            return

        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        with self._socket_lock:
            if self._stop_event.is_set():
                receiver.close()
                return
            self._socket = receiver

        try:
            receiver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            receiver.settimeout(RECEIVE_TIMEOUT_SECONDS)
            receiver.bind((self._host, self._port))
            logging.info(
                "heartbeat_receiver_listen | host=%s | port=%s",
                self._host,
                self._port,
            )

            while not self._stop_event.is_set():
                try:
                    payload, _ = receiver.recvfrom(MAX_HEARTBEAT_BYTES)
                except socket.timeout:
                    continue
                except OSError:
                    if self._stop_event.is_set():
                        return
                    raise
                self._record(payload)
        finally:
            with self._socket_lock:
                if self._socket is receiver:
                    self._socket = None
            receiver.close()

    def last_seen(self, node_id: str) -> float | None:
        with self._last_seen_lock:
            return self._last_seen.get(node_id)

    def stop(self) -> None:
        self._stop_event.set()
        with self._socket_lock:
            receiver = self._socket
        if receiver is not None:
            try:
                receiver.close()
            except OSError:
                pass

    def _record(self, payload: bytes) -> None:
        try:
            node_id = payload.decode("utf-8")
        except UnicodeDecodeError:
            logging.warning("heartbeat_receiver_invalid_utf8")
            return

        if not node_id:
            logging.warning("heartbeat_receiver_empty_node_id")
            return

        timestamp = self._clock()
        with self._last_seen_lock:
            self._last_seen[node_id] = timestamp
