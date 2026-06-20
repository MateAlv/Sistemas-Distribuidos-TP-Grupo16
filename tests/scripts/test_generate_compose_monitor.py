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


def test_file_ingestors_use_sharded_exchange_and_personal_queue() -> None:
    config = _config({"enabled": False})
    config["workers"]["file_splitters"] = 1
    config["workers"]["file_ingestors"] = 2

    services = generate_compose.build_compose(config, expose_ports=False)["services"]

    splitter_env = _env(services["file_splitter_0"])
    assert splitter_env["LINE_BATCH_OUTPUT_EXCHANGE"] == "line_batch_exchange"
    assert splitter_env["LINE_BATCH_OUTPUT_ROUTING_PREFIX"] == "file_ingestor"
    assert splitter_env["FILE_INGESTOR_AMOUNT"] == "2"
    assert "LINE_BATCH_OUTPUT_QUEUE" not in splitter_env

    ingestor_0_env = _env(services["file_ingestor_0"])
    assert ingestor_0_env["LINE_BATCH_INPUT_EXCHANGE"] == "line_batch_exchange"
    assert ingestor_0_env["LINE_BATCH_INPUT_ROUTING_PREFIX"] == "file_ingestor"
    assert ingestor_0_env["LINE_BATCH_INPUT_QUEUE"] == "file_ingestor_0"
    assert ingestor_0_env["FILTER_USD_EXCHANGE"] == "filter_usd_exchange"
    assert ingestor_0_env["FILTER_USD_ROUTING_PREFIX"] == "filter_usd"
    assert ingestor_0_env["FILTER_USD_AMOUNT"] == "1"
    assert "FILTER_Q5_FORMAT_EXCHANGE" not in ingestor_0_env
    assert "TRANSACTION_OUTPUT_EXCHANGE" not in ingestor_0_env

    ingestor_1_env = _env(services["file_ingestor_1"])
    assert ingestor_1_env["LINE_BATCH_INPUT_EXCHANGE"] == "line_batch_exchange"
    assert ingestor_1_env["LINE_BATCH_INPUT_ROUTING_PREFIX"] == "file_ingestor"
    assert ingestor_1_env["LINE_BATCH_INPUT_QUEUE"] == "file_ingestor_1"


def test_file_ingestor_dual_outputs_and_filter_personal_inputs() -> None:
    config = _config({"enabled": False})
    config["queries"] = ["q1", "q5"]
    config["workers"]["file_ingestors"] = 1
    config["workers"]["filters"] = {
        "usd": 2,
        "q1": 1,
        "q5_format": 3,
        "q5_usd": 1,
    }
    config["workers"]["aggregators"] = {"q5": 1}
    config["workers"]["joiners"] = {"q5": 1}

    services = generate_compose.build_compose(config, expose_ports=False)["services"]

    ingestor_env = _env(services["file_ingestor_0"])
    assert ingestor_env["FILTER_USD_EXCHANGE"] == "filter_usd_exchange"
    assert ingestor_env["FILTER_USD_ROUTING_PREFIX"] == "filter_usd"
    assert ingestor_env["FILTER_USD_AMOUNT"] == "2"
    assert ingestor_env["FILTER_Q5_FORMAT_EXCHANGE"] == "filter_q5_format_exchange"
    assert ingestor_env["FILTER_Q5_FORMAT_ROUTING_PREFIX"] == "filter_q5_format"
    assert ingestor_env["FILTER_Q5_FORMAT_AMOUNT"] == "3"
    assert "TRANSACTION_OUTPUT_EXCHANGE" not in ingestor_env

    filter_usd_1_env = _env(services["filter_usd_1"])
    assert filter_usd_1_env["INPUT_QUEUE"] == "filter_usd_1"
    assert filter_usd_1_env["INPUT_EXCHANGE"] == "filter_usd_exchange"
    assert filter_usd_1_env["INPUT_ROUTING_PREFIX"] == "filter_usd"
    assert "TRANSACTION_EXCHANGE" not in filter_usd_1_env

    filter_q5_format_2_env = _env(services["filter_q5_format_2"])
    assert filter_q5_format_2_env["INPUT_QUEUE"] == "filter_q5_format_2"
    assert filter_q5_format_2_env["INPUT_EXCHANGE"] == "filter_q5_format_exchange"
    assert filter_q5_format_2_env["INPUT_ROUTING_PREFIX"] == "filter_q5_format"
    assert "TRANSACTION_EXCHANGE" not in filter_q5_format_2_env


def test_filter_usd_outputs_to_q1_and_date_personal_inputs() -> None:
    config = _config({"enabled": False})
    config["queries"] = ["q1", "q3"]
    config["workers"]["filters"] = {"usd": 2, "q1": 3, "date": 4}
    config["workers"]["sums"] = {"q3": 1}
    config["workers"]["aggregators"] = {"q3": 1}
    config["workers"]["joiners"] = {"q3": 1}

    services = generate_compose.build_compose(config, expose_ports=False)["services"]

    filter_usd_0_env = _env(services["filter_usd_0"])
    assert filter_usd_0_env["FILTER_Q1_EXCHANGE"] == "filter_q1_exchange"
    assert filter_usd_0_env["FILTER_Q1_ROUTING_PREFIX"] == "filter_q1"
    assert filter_usd_0_env["FILTER_Q1_AMOUNT"] == "3"
    assert filter_usd_0_env["FILTER_DATE_EXCHANGE"] == "filter_date_exchange"
    assert filter_usd_0_env["FILTER_DATE_ROUTING_PREFIX"] == "filter_date"
    assert filter_usd_0_env["FILTER_DATE_AMOUNT"] == "4"

    filter_q1_2_env = _env(services["filter_q1_2"])
    assert filter_q1_2_env["INPUT_QUEUE"] == "filter_q1_2"
    assert filter_q1_2_env["INPUT_EXCHANGE"] == "filter_q1_exchange"
    assert filter_q1_2_env["INPUT_ROUTING_PREFIX"] == "filter_q1"

    filter_date_3_env = _env(services["filter_date_3"])
    assert filter_date_3_env["INPUT_QUEUE"] == "filter_date_3"
    assert filter_date_3_env["INPUT_EXCHANGE"] == "filter_date_exchange"
    assert filter_date_3_env["INPUT_ROUTING_PREFIX"] == "filter_date"


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
