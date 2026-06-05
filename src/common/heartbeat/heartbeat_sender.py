import logging
import os
import socket
import threading

HEARTBEAT_INTERVAL_SECONDS = 1.0
DEFAULT_MONITOR_HOST = "monitor"
DEFAULT_MONITOR_PORT = 9000


class HeartbeatSender:
    """Periodically emits this process's node id to the Monitor over UDP.

    Runs on a daemon thread and stops
    on stop() (wired to SIGTERM by the worker entrypoint).
    """

    def __init__(self, node_id=None, host=None, port=None, interval=None):
        self._node_id = node_id or os.getenv("NODE_NAME") or socket.gethostname()
        self._host = host or os.getenv("MONITOR_HOST", DEFAULT_MONITOR_HOST)
        self._port = int(port if port is not None else os.getenv("MONITOR_PORT", DEFAULT_MONITOR_PORT))
        self._interval = float(
            interval if interval is not None
            else os.getenv("HEARTBEAT_INTERVAL_SECONDS", HEARTBEAT_INTERVAL_SECONDS)
        )
        self._payload = self._node_id.encode()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="heartbeat-sender", daemon=True)
        self._send_failing = False

    def start(self):
        logging.info(
            "heartbeat_sender | action: start | node: %s | target: %s:%s | interval: %ss",
            self._node_id, self._host, self._port, self._interval,
        )
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def _run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            while not self._stop_event.is_set():
                self._send(sock)
                self._stop_event.wait(self._interval)
        finally:
            sock.close()
            logging.info("heartbeat_sender | action: stop | node: %s", self._node_id)

    def _send(self, sock):
        try:
            sent = sock.sendto(self._payload, (self._host, self._port))
            if sent != len(self._payload):
                logging.warning(
                    "heartbeat_sender | partial send: %s/%s bytes", sent, len(self._payload)
                )
            if self._send_failing:
                logging.info("heartbeat_sender | send recovered to %s:%s", self._host, self._port)
                self._send_failing = False
        except OSError as exc:
            if not self._send_failing:
                logging.warning("heartbeat_sender | send failing (will keep retrying): %s", exc)
                self._send_failing = True
