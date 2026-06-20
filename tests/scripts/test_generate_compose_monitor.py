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


def test_build_compose_adds_worker_state_volumes_and_durable_env() -> None:
    compose = generate_compose.build_compose(
        _config({"enabled": False}),
        expose_ports=False,
    )
    services = compose["services"]
    volumes = compose["volumes"]

    worker_names = {
        "file_splitter_0",
        "file_ingestor_0",
        "filter_usd_0",
        "filter_q1_0",
    }
    for name in worker_names:
        env = _env(services[name])
        volume_name = f"{name}_state"
        assert env["RABBITMQ_DURABLE"] == "true"
        assert env["STATE_DIR"] == "/worker_state"
        assert env["SNAPSHOT_INTERVAL"] == "1000"
        assert services[name]["volumes"] == [f"{volume_name}:/worker_state"]
        assert volume_name in volumes

    gateway_env = _env(services["gateway"])
    assert gateway_env["RABBITMQ_DURABLE"] == "true"
    assert "STATE_DIR" not in gateway_env
    assert "volumes" not in services["gateway"]

    client_env = _env(services["client_0"])
    assert "RABBITMQ_DURABLE" not in client_env
    assert "STATE_DIR" not in client_env

    assert "RABBITMQ_DURABLE" not in _env(services["rabbitmq"])


def test_rates_service_is_durable_but_has_no_worker_state_volume() -> None:
    config = _config({"enabled": False})
    config["queries"] = ["q5"]

    services = generate_compose.build_compose(config, expose_ports=False)["services"]

    env = _env(services["rates_service"])
    assert env["RABBITMQ_DURABLE"] == "true"
    assert "STATE_DIR" not in env
    assert services["rates_service"]["volumes"] == ["./data/rates:/data/rates:rw"]


def test_q2_sum_uses_sharded_exchange_and_personal_queue() -> None:
    config = _config({"enabled": False})
    config["queries"] = ["q2"]
    config["workers"]["sums"] = {"q2": 2}
    config["workers"]["aggregators"] = {"q2": 1}
    config["workers"]["joiners"] = {"q2": 1}

    services = generate_compose.build_compose(config, expose_ports=False)["services"]

    filter_env = _env(services["filter_usd_0"])
    assert filter_env["SUM_Q2_EXCHANGE"] == "sum_q2_exchange"
    assert filter_env["SUM_Q2_ROUTING_PREFIX"] == "sum_q2"
    assert filter_env["SUM_Q2_AMOUNT"] == "2"
    assert "SUM_Q2_QUEUE" not in filter_env

    sum_0_env = _env(services["sum_q2_0"])
    assert sum_0_env["INPUT_EXCHANGE"] == "sum_q2_exchange"
    assert sum_0_env["INPUT_ROUTING_PREFIX"] == "sum_q2"
    assert sum_0_env["INPUT_QUEUE"] == "sum_q2_0"

    sum_1_env = _env(services["sum_q2_1"])
    assert sum_1_env["INPUT_EXCHANGE"] == "sum_q2_exchange"
    assert sum_1_env["INPUT_ROUTING_PREFIX"] == "sum_q2"
    assert sum_1_env["INPUT_QUEUE"] == "sum_q2_1"


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
