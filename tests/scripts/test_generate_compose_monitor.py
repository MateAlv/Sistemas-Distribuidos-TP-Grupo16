from pathlib import Path

import pytest

from scripts import generate_compose


def _config(monitor=None):
    return {
        "queries": ["q1"],
        "settings": {"logging_level": "INFO"},
        "monitor": monitor or {"enabled": True, "count": 3},
        "workers": {
            "file_splitters": 1,
            "file_ingestors": 1,
            "filters": {"usd": 1, "q1": 1},
        },
        "clients": 1,
        "client_accounts": [
            {
                "client_id": 0,
                "accounts_file": "data/sample/accounts.csv",
                "transactions_file": "data/sample/transactions.csv",
            }
        ],
    }


def _env(service):
    return dict(item.split("=", 1) for item in service["environment"])


def test_build_compose_adds_monitor_replicas_and_heartbeat_targets() -> None:
    services = generate_compose.build_compose(
        _config(),
        expose_ports=False,
    )["services"]

    assert {"monitor_1", "monitor_2", "monitor_3"} <= services.keys()
    monitor_hosts = "monitor_1,monitor_2,monitor_3"
    expected_nodes = {
        "file_splitter_0",
        "file_ingestor_0",
        "filter_usd_0",
        "filter_q1_0",
        "monitor_1",
        "monitor_2",
        "monitor_3",
    }

    for monitor_id in range(1, 4):
        name = f"monitor_{monitor_id}"
        service = services[name]
        env = _env(service)
        assert service["container_name"] == name
        assert service["volumes"] == [
            "/var/run/docker.sock:/var/run/docker.sock",
            f"./data/monitor/monitor_{monitor_id}:/data/monitor:rw",
        ]
        assert env["MONITOR_ID"] == str(monitor_id)
        assert env["MONITOR_COUNT"] == "3"
        assert env["MONITOR_HOSTS"] == monitor_hosts
        assert env["MONITOR_STATE_PATH"] == "/data/monitor/epoch.json"
        assert env["STARTUP_GRACE_PERIOD"] == "30.0"
        assert env["PINNED_CONTAINER_NAMES"] == "true"
        assert set(env["NODES_TO_WATCH"].split(",")) == expected_nodes

    for name in expected_nodes:
        env = _env(services[name])
        assert env["MONITOR_HOSTS"] == monitor_hosts
        assert env["MONITOR_PORT"] == "9000"

    assert "MONITOR_HOSTS" not in _env(services["gateway"])
    assert "MONITOR_HOSTS" not in _env(services["client_0"])


def test_build_compose_omits_monitors_when_disabled() -> None:
    services = generate_compose.build_compose(
        _config({"enabled": False}),
        expose_ports=False,
    )["services"]

    assert not any(name.startswith("monitor_") for name in services)
    assert "MONITOR_HOSTS" not in _env(services["filter_usd_0"])


@pytest.mark.parametrize(
    ("monitor", "message"),
    [
        ({"enabled": True, "count": 0}, "monitor.count"),
        ({"enabled": True, "count": 256}, "monitor.count"),
        ({"enabled": True, "port": 0}, "monitor.port"),
        (
            {"enabled": True, "check_interval": 0},
            "monitor.check_interval",
        ),
        ({"enabled": True, "max_missed": 0}, "monitor.max_missed"),
        (
            {"enabled": True, "startup_grace_period": -1},
            "monitor.startup_grace_period",
        ),
    ],
)
def test_validate_config_rejects_invalid_monitor_settings(
    tmp_path,
    monkeypatch,
    monitor,
    message,
) -> None:
    accounts = tmp_path / "data/sample/accounts.csv"
    transactions = tmp_path / "data/sample/transactions.csv"
    accounts.parent.mkdir(parents=True)
    accounts.touch()
    transactions.touch()
    monkeypatch.setattr(generate_compose, "ROOT", tmp_path)

    with pytest.raises(ValueError, match=message):
        generate_compose.validate_config(
            _config(monitor),
            Path("test-config.yaml"),
        )
