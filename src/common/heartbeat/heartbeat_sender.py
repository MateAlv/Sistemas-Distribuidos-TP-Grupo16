import logging
import os
import socket
import threading

HEARTBEAT_INTERVAL_SECONDS = 1.0
DEFAULT_MONITOR_HOST = "monitor"
DEFAULT_MONITOR_PORT = 9000


class HeartbeatSender:
    """Periodically emits this process's node id to every Monitor over UDP.

    Runs on a daemon thread and stops on stop() (wired to SIGTERM by the worker
    entrypoint).
    """

    def __init__(
        self,
        node_id=None,
        host=None,
        hosts=None,
        port=None,
        interval=None,
    ):
        self._node_id = node_id or os.getenv("NODE_NAME") or socket.gethostname()
        self._hosts = _monitor_hosts(host, hosts)
        self._host = self._hosts[0]
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
            "heartbeat_sender | action: start | node: %s | targets: %s | "
            "port: %s | interval: %ss",
            self._node_id,
            ",".join(self._hosts),
            self._port,
            self._interval,
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
        failed = False
        for host in self._hosts:
            try:
                sent = sock.sendto(self._payload, (host, self._port))
                if sent != len(self._payload):
                    logging.warning(
                        "heartbeat_sender | partial send: %s/%s bytes | target: %s:%s",
                        sent,
                        len(self._payload),
                        host,
                        self._port,
                    )
            except OSError as exc:
                failed = True
                if not self._send_failing:
                    logging.warning(
                        "heartbeat_sender | send failing | target: %s:%s | "
                        "error: %s",
                        host,
                        self._port,
                        exc,
                    )

        if self._send_failing and not failed:
            logging.info("heartbeat_sender | send recovered")
        self._send_failing = failed


def _monitor_hosts(host, hosts):
    if hosts is not None:
        values = hosts.split(",") if isinstance(hosts, str) else hosts
    elif host is not None:
        values = [host]
    else:
        configured = os.getenv("MONITOR_HOSTS")
        values = (
            configured.split(",")
            if configured is not None
            else [os.getenv("MONITOR_HOST", DEFAULT_MONITOR_HOST)]
        )

    normalized = tuple(dict.fromkeys(value.strip() for value in values if value.strip()))
    if not normalized:
        return (DEFAULT_MONITOR_HOST,)
    return normalized
