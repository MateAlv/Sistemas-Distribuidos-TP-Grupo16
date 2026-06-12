import signal

import pytest

import monitor.main as monitor_main


def _required_env(**overrides) -> dict[str, str]:
    env = {
        "MONITOR_ID": "2",
        "MONITOR_COUNT": "3",
        "NODES_TO_WATCH": "worker_0, worker_1,monitor_1",
    }
    env.update(overrides)
    return env


def test_load_config_uses_defaults_and_deduplicates_nodes() -> None:
    config = monitor_main.load_config(
        _required_env(
            NODES_TO_WATCH="worker_0, worker_0, monitor_1",
        )
    )

    assert config.monitor_id == 2
    assert config.monitor_count == 3
    assert config.monitor_port == 9000
    assert config.election_port == 9001
    assert config.check_interval == 3.0
    assert config.max_missed == 3
    assert config.nodes_to_watch == ("worker_0", "monitor_1")
    assert config.heartbeat_target_host == "monitor"


def test_load_config_accepts_all_runtime_overrides() -> None:
    config = monitor_main.load_config(
        _required_env(
            MONITOR_BIND_HOST="127.0.0.1",
            MONITOR_PORT="9100",
            ELECTION_BIND_HOST="127.0.0.2",
            ELECTION_PORT="9101",
            MONITOR_CHECK_INTERVAL="1.5",
            MAX_MISSED="4",
            ELECTION_TIMEOUT="2.5",
            COORDINATOR_TIMEOUT="6",
            MONITOR_HOST="monitor_1",
            LOGGING_LEVEL="DEBUG",
        )
    )

    assert config.monitor_host == "127.0.0.1"
    assert config.monitor_port == 9100
    assert config.election_host == "127.0.0.2"
    assert config.election_port == 9101
    assert config.check_interval == 1.5
    assert config.max_missed == 4
    assert config.election_timeout == 2.5
    assert config.coordinator_timeout == 6
    assert config.heartbeat_target_host == "monitor_1"
    assert config.logging_level == "DEBUG"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"MONITOR_ID": None}, "MONITOR_ID is required"),
        ({"MONITOR_COUNT": None}, "MONITOR_COUNT is required"),
        ({"NODES_TO_WATCH": None}, "NODES_TO_WATCH is required"),
        ({"MONITOR_ID": "4"}, "MONITOR_ID must be in range"),
        ({"MONITOR_COUNT": "256"}, "MONITOR_COUNT must be in range"),
        ({"MONITOR_PORT": "0"}, "MONITOR_PORT must be in range"),
        ({"ELECTION_PORT": "70000"}, "ELECTION_PORT must be in range"),
        ({"MAX_MISSED": "0"}, "MAX_MISSED must be greater than 0"),
        (
            {"MONITOR_CHECK_INTERVAL": "0"},
            "MONITOR_CHECK_INTERVAL must be greater than 0",
        ),
        ({"NODES_TO_WATCH": " , "}, "must contain at least one node"),
    ],
)
def test_load_config_rejects_invalid_values(overrides, message) -> None:
    env = _required_env()
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value

    with pytest.raises(ValueError, match=message):
        monitor_main.load_config(env)


def test_builders_wire_config_into_components(monkeypatch) -> None:
    calls = {}

    class FakeElection:
        def __init__(self, **kwargs):
            calls["election"] = kwargs

    class FakeReceiver:
        def __init__(self, **kwargs):
            calls["receiver"] = kwargs

    class FakeMonitor:
        def __init__(self, **kwargs):
            calls["monitor"] = kwargs

    class FakeSender:
        def __init__(self, **kwargs):
            calls["sender"] = kwargs

    monkeypatch.setattr(monitor_main, "ElectionHandler", FakeElection)
    monkeypatch.setattr(monitor_main, "HeartbeatReceiver", FakeReceiver)
    monkeypatch.setattr(monitor_main, "Monitor", FakeMonitor)
    monkeypatch.setattr(monitor_main, "HeartbeatSender", FakeSender)
    config = monitor_main.load_config(_required_env())

    monitor_main.build_monitor(config)
    monitor_main.build_heartbeat_sender(config)

    assert calls["election"]["monitor_id"] == 2
    assert calls["election"]["monitor_count"] == 3
    assert calls["receiver"] == {"host": "0.0.0.0", "port": 9000}
    assert calls["monitor"]["nodes_to_watch"] == (
        "worker_0",
        "worker_1",
        "monitor_1",
    )
    assert calls["sender"] == {
        "node_id": "monitor_2",
        "host": "monitor",
        "port": 9000,
    }


def test_signal_handlers_stop_monitor_and_heartbeat(monkeypatch) -> None:
    handlers = {}

    class Stoppable:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    monitor = Stoppable()
    heartbeat = Stoppable()
    monkeypatch.setattr(
        monitor_main.signal,
        "signal",
        lambda signum, handler: handlers.setdefault(signum, handler),
    )

    monitor_main.install_signal_handlers(monitor, heartbeat)
    handlers[signal.SIGTERM](signal.SIGTERM, None)

    assert monitor.stop_calls == 1
    assert heartbeat.stop_calls == 1
    assert signal.SIGINT in handlers


def test_main_starts_and_stops_components(monkeypatch) -> None:
    events = []
    config = monitor_main.load_config(_required_env())

    class FakeMonitor:
        def run(self):
            events.append("monitor.run")

        def stop(self):
            events.append("monitor.stop")

    class FakeHeartbeat:
        def start(self):
            events.append("heartbeat.start")

        def stop(self):
            events.append("heartbeat.stop")

    monkeypatch.setattr(
        monitor_main,
        "load_config",
        lambda: config,
    )
    monkeypatch.setattr(monitor_main, "initialize_log", lambda _level: None)
    monkeypatch.setattr(monitor_main, "build_monitor", lambda _config: FakeMonitor())
    monkeypatch.setattr(
        monitor_main,
        "build_heartbeat_sender",
        lambda _config: FakeHeartbeat(),
    )
    monkeypatch.setattr(
        monitor_main,
        "install_signal_handlers",
        lambda *_args: None,
    )

    assert monitor_main.main() == 0
    assert events == [
        "heartbeat.start",
        "monitor.run",
        "heartbeat.stop",
        "monitor.stop",
    ]


def test_main_returns_config_error(monkeypatch) -> None:
    monkeypatch.setattr(
        monitor_main,
        "load_config",
        lambda: (_ for _ in ()).throw(ValueError("bad config")),
    )

    assert monitor_main.main() == 2


def test_main_stops_components_when_monitor_run_fails(monkeypatch) -> None:
    events = []
    config = monitor_main.load_config(_required_env())

    class FailingMonitor:
        def run(self):
            events.append("monitor.run")
            raise RuntimeError("monitor failed")

        def stop(self):
            events.append("monitor.stop")

    class FakeHeartbeat:
        def start(self):
            events.append("heartbeat.start")

        def stop(self):
            events.append("heartbeat.stop")

    monkeypatch.setattr(monitor_main, "load_config", lambda: config)
    monkeypatch.setattr(monitor_main, "initialize_log", lambda _level: None)
    monkeypatch.setattr(
        monitor_main,
        "build_monitor",
        lambda _config: FailingMonitor(),
    )
    monkeypatch.setattr(
        monitor_main,
        "build_heartbeat_sender",
        lambda _config: FakeHeartbeat(),
    )
    monkeypatch.setattr(
        monitor_main,
        "install_signal_handlers",
        lambda *_args: None,
    )

    with pytest.raises(RuntimeError, match="monitor failed"):
        monitor_main.main()

    assert events == [
        "heartbeat.start",
        "monitor.run",
        "heartbeat.stop",
        "monitor.stop",
    ]
