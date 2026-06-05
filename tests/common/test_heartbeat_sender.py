import socket

from common.heartbeat import HeartbeatSender
from common.heartbeat.heartbeat_sender import DEFAULT_MONITOR_HOST, DEFAULT_MONITOR_PORT


def _receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.settimeout(2)
    return sock, sock.getsockname()[1]


def test_sends_node_id_as_plain_bytes():
    rx, port = _receiver()
    sender = HeartbeatSender(node_id="filter_usd_3", host="127.0.0.1", port=port, interval=0.05)
    sender.start()
    try:
        data, _ = rx.recvfrom(64)
    finally:
        sender.stop()
        rx.close()
    assert data == b"filter_usd_3"


def test_node_id_defaults_to_node_name_env(monkeypatch):
    monkeypatch.setenv("NODE_NAME", "q4_sum_7")
    rx, port = _receiver()
    sender = HeartbeatSender(host="127.0.0.1", port=port, interval=0.05)
    sender.start()
    try:
        data, _ = rx.recvfrom(64)
    finally:
        sender.stop()
        rx.close()
    assert data == b"q4_sum_7"


def test_node_id_falls_back_to_hostname(monkeypatch):
    monkeypatch.delenv("NODE_NAME", raising=False)
    sender = HeartbeatSender(host="127.0.0.1", port=9)
    assert sender._payload == socket.gethostname().encode()


def test_defaults_target_monitor(monkeypatch):
    monkeypatch.delenv("MONITOR_HOST", raising=False)
    monkeypatch.delenv("MONITOR_PORT", raising=False)
    sender = HeartbeatSender(node_id="x")
    assert sender._host == DEFAULT_MONITOR_HOST
    assert sender._port == DEFAULT_MONITOR_PORT


def test_emits_repeatedly():
    rx, port = _receiver()
    sender = HeartbeatSender(node_id="x", host="127.0.0.1", port=port, interval=0.05)
    sender.start()
    try:
        first, _ = rx.recvfrom(64)
        second, _ = rx.recvfrom(64)
    finally:
        sender.stop()
        rx.close()
    assert first == b"x"
    assert second == b"x"


def test_survives_unreachable_monitor():
    sender = HeartbeatSender(node_id="x", host="monitor.invalid", port=9000, interval=0.02)
    sender.start()
    try:
        sender._thread.join(timeout=0.2)
        assert sender._thread.is_alive()
    finally:
        sender.stop()
    sender._thread.join(timeout=1)
    assert not sender._thread.is_alive()


def test_stop_halts_thread():
    rx, port = _receiver()
    sender = HeartbeatSender(node_id="x", host="127.0.0.1", port=port, interval=0.05)
    sender.start()
    try:
        rx.recvfrom(64)
    finally:
        rx.close()
    sender.stop()
    sender._thread.join(timeout=1)
    assert not sender._thread.is_alive()
