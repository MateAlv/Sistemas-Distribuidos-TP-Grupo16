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
        _required_env(NODES_TO_WATCH="worker_0, worker_0, monitor_1")
    )

    assert config.monitor_id == 2
    assert config.monitor_count == 3
    assert config.monitor_port == 9000
    assert config.election_port == 9001
    assert config.check_interval == 3.0
    assert config.max_missed == 3
    assert config.nodes_to_watch == ("worker_0", "monitor_1")
    assert config.heartbeat_target_hosts == ("monitor",)


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
            MONITOR_HOSTS="monitor_1,monitor_2",
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
    assert config.heartbeat_target_hosts == ("monitor_1", "monitor_2")
    assert config.logging_level == "DEBUG"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"MONITOR_ID": None}, "MONITOR_ID is required"),
        ({"NODES_TO_WATCH": None}, "NODES_TO_WATCH is required"),
        ({"MONITOR_ID": "4"}, "MONITOR_ID must be in range"),
        ({"MONITOR_COUNT": "256"}, "MONITOR_COUNT must be in range"),
        ({"MONITOR_PORT": "0"}, "MONITOR_PORT must be in range"),
        ({"MAX_MISSED": "0"}, "MAX_MISSED must be greater than 0"),
        ({"MONITOR_CHECK_INTERVAL": "0"}, "MONITOR_CHECK_INTERVAL must be greater than 0"),
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

    monkeypatch.setattr(monitor_main, "load_config", lambda: config)
    monkeypatch.setattr(monitor_main, "build_monitor", lambda _config: FakeMonitor())
    monkeypatch.setattr(monitor_main, "build_heartbeat_sender", lambda _config: FakeHeartbeat())

    assert monitor_main.main() == 0
    assert events == ["heartbeat.start", "monitor.run", "heartbeat.stop", "monitor.stop"]


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
    monkeypatch.setattr(monitor_main, "build_monitor", lambda _config: FailingMonitor())
    monkeypatch.setattr(monitor_main, "build_heartbeat_sender", lambda _config: FakeHeartbeat())

    with pytest.raises(RuntimeError, match="monitor failed"):
        monitor_main.main()

    assert events == ["heartbeat.start", "monitor.run", "heartbeat.stop", "monitor.stop"]
