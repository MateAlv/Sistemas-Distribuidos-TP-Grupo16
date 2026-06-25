import socket
import threading

from monitor.heartbeat import HeartbeatReceiver
from monitor.heartbeat import heartbeat_receiver


class FakeDatagramSocket:
    def __init__(self, payloads: list[bytes] | None = None) -> None:
        self.payloads = list(payloads or [])
        self.bound_address = None
        self.socket_options = []
        self.timeout = None
        self.closed = False
        self.recv_started = threading.Event()
        self.waiting_for_payload = threading.Event()
        self._closed_event = threading.Event()

    def setsockopt(self, level: int, option: int, value: int) -> None:
        self.socket_options.append((level, option, value))

    def bind(self, address: tuple[str, int]) -> None:
        self.bound_address = address

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
        self.recv_started.set()
        if self.payloads:
            return self.payloads.pop(0), ("worker", 0)
        self.waiting_for_payload.set()
        self._closed_event.wait(timeout=1)
        raise OSError("socket closed")

    def close(self) -> None:
        self.closed = True
        self._closed_event.set()


def _run_receiver(receiver: HeartbeatReceiver) -> threading.Thread:
    thread = threading.Thread(target=receiver.listen)
    thread.start()
    return thread


def test_records_latest_timestamp_for_each_node(monkeypatch) -> None:
    fake_socket = FakeDatagramSocket(
        [b"filter_usd_0", b"q4_sum_1", b"filter_usd_0"]
    )
    monkeypatch.setattr(heartbeat_receiver.socket, "socket", lambda *_: fake_socket)
    timestamps = iter([10.0, 20.0, 30.0])
    receiver = HeartbeatReceiver(
        host="127.0.0.1",
        port=9876,
        clock=lambda: next(timestamps),
    )

    thread = _run_receiver(receiver)
    assert fake_socket.waiting_for_payload.wait(timeout=1)
    receiver.stop()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert receiver.last_seen("filter_usd_0") == 30.0
    assert receiver.last_seen("q4_sum_1") == 20.0
    assert receiver.last_seen("missing") is None
    assert fake_socket.bound_address == ("127.0.0.1", 9876)
    assert fake_socket.timeout == 0.5
    assert (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) in fake_socket.socket_options


def test_ignores_empty_and_invalid_utf8_payloads(monkeypatch) -> None:
    fake_socket = FakeDatagramSocket([b"", b"\xff", b"worker_0"])
    monkeypatch.setattr(heartbeat_receiver.socket, "socket", lambda *_: fake_socket)
    receiver = HeartbeatReceiver(
        clock=lambda: 42.0,
    )

    thread = _run_receiver(receiver)
    assert fake_socket.waiting_for_payload.wait(timeout=1)
    receiver.stop()
    thread.join(timeout=1)

    assert receiver.last_seen("") is None
    assert receiver.last_seen("worker_0") == 42.0


def test_stop_unblocks_receive_and_is_idempotent(monkeypatch) -> None:
    fake_socket = FakeDatagramSocket()
    monkeypatch.setattr(heartbeat_receiver.socket, "socket", lambda *_: fake_socket)
    receiver = HeartbeatReceiver()

    thread = _run_receiver(receiver)
    assert fake_socket.recv_started.wait(timeout=1)

    receiver.stop()
    receiver.stop()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert fake_socket.closed


def test_stop_before_listen_does_not_create_socket(monkeypatch) -> None:
    created = []
    monkeypatch.setattr(
        heartbeat_receiver.socket,
        "socket",
        lambda *_: created.append(True),
    )
    receiver = HeartbeatReceiver()

    receiver.stop()
    receiver.listen()

    assert created == []
