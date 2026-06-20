#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "main-config.yaml"
DEFAULT_COMPOSE = ROOT / "docker-compose.yaml"
DEFAULT_TEST_COMPOSE = ROOT / "docker-compose.test.yaml"

MOM_HOST = "rabbitmq"
SERVER_HOST = "gateway"
SERVER_PORT = 5678
FILE_INGESTOR_EXCHANGE = "file_ingestor_exchange"
FILE_SPLITTER_QUEUE_PREFIX = "file_splitter"
TRANSACTION_EXCHANGE = "transaction_fanout_exchange"
FILE_INGESTOR_CONTROL_EXCHANGE = "file_ingestor_control"
FILE_INGESTOR_RESPONSE_QUEUE_PREFIX = "file_ingestor_response"
FILTER_PREFIX = "filter"

LINE_BATCH_QUEUE = "line_batch_queue"
FILTER_USD_QUEUE = "filter_usd_queue"
FILTER_Q1_QUEUE = "filter_q1_queue"
FILTER_DATE_QUEUE = "filter_date_queue"
FILTER_Q3_QUEUE = "filter_q3_queue"
Q3_CANDIDATES_QUEUE = "q3_candidates_queue"
FILTER_Q5_FORMAT_QUEUE = "filter_q5_format_queue"
FILTER_Q5_USD_QUEUE = "filter_q5_usd_queue"
SUM_Q2_QUEUE = "sum_q2_queue"
SUM_Q3_QUEUE = "sum_q3_queue"
SUM_Q2_EXCHANGE = "sum_q2_exchange"
GATEWAY_Q1_QUEUE = "gateway_results_queue"
GATEWAY_Q2_QUEUE = "join_q2_results_queue"
GATEWAY_Q3_QUEUE = "gateway_q3_results_queue"
GATEWAY_Q4_QUEUE = "gateway_q4_results_queue"
GATEWAY_Q5_QUEUE = "join_q5_results_queue"
RATES_REQUEST_QUEUE = "rates_requests"

SUM_Q2_PREFIX = "sum_q2"
SUM_Q3_PREFIX = "sum_q3"
AGGREGATION_Q2_PREFIX = "aggregation_q2"
AGGREGATION_Q3_PREFIX = "aggregation_q3"
AGGREGATION_Q5_PREFIX = "aggregation_q5"
JOIN_Q2_QUEUE = "join_q2_queue"
JOIN_Q3_QUEUE = "join_q3_queue"
JOIN_Q3_RESULTS_QUEUE = "join_q3_results_queue"
JOIN_Q5_QUEUE = "join_q5_queue"

Q2_ENRICH_QUEUE = "q2_enrich_queue"
ACCOUNTS_LINE_BATCH_QUEUE = "accounts_line_batch_queue"

SG_MAPPER_QUEUE = "scatter_gather_mapper_queue"
SG_LINKER_EXCHANGE = "sg_linker_exchange"
SG_DETECTOR_EXCHANGE = "sg_detector_exchange"
Q4_FILTER_INPUT_EXCHANGE = "q4_filter_input_exchange"
Q4_FILTER_ROUTING_PREFIX = "q4_filter"
Q4_SUM_EXCHANGE = "q4_sum_exchange"
Q4_SUM_ROUTING_PREFIX = "q4_sum"
Q4_JOINER_EXCHANGE = "q4_joiner_exchange"
Q4_JOINER_ROUTING_PREFIX = "q4_joiner"
Q4_AGGREGATOR_EXCHANGE = "q4_aggregator_exchange"
Q4_AGGREGATOR_ROUTING_PREFIX = "q4_aggregator"
Q4_DEDUPER_EXCHANGE = "q4_deduper_exchange"
Q4_DEDUPER_ROUTING_PREFIX = "q4_deduper"
Q4_DEDUPER_RESPONSE_QUEUE_PREFIX = "q4_deduper_response"
Q3_AVERAGES_EXCHANGE = "q3_averages_exchange"
Q3_CANDIDATES_EXCHANGE = "q3_candidates_exchange"
Q3_AVERAGES_ROUTING_PREFIX = "q3_averages"
Q3_CANDIDATES_ROUTING_PREFIX = "q3_candidates"
WORKER_STATE_DIR = "/worker_state"
DEFAULT_SNAPSHOT_INTERVAL = 1000
RABBITMQ_DURABLE_ENV = "RABBITMQ_DURABLE=true"
OBSERVABILITY_DEFAULTS = {
    "FLOW_LOG_ENABLED": "1",
    "FLOW_LOG_EVERY_MESSAGES": "100000",
    "FLOW_LOG_EVERY_BYTES": str(2 * 1024 * 1024 * 1024),
    "FLOW_LOG_FIRST_MESSAGES": "1",
    "WORKER_LOG_EVERY_MESSAGES": "100000",
    "CHUNK_LOG_EVERY": "10000",
    "RESULT_LOG_EVERY": "100000",
}
DEFAULT_MONITOR_COUNT = 3
DEFAULT_MONITOR_PORT = 9000
DEFAULT_ELECTION_PORT = 9001
DEFAULT_MONITOR_CHECK_INTERVAL = 3.0
DEFAULT_MAX_MISSED = 3
DEFAULT_ELECTION_TIMEOUT = 5.0
DEFAULT_COORDINATOR_TIMEOUT = 10.0
DEFAULT_STARTUP_GRACE_PERIOD = 30.0


def main() -> int:
    args = parse_args()
    if args.preset:
        config = preset_config(
            args.preset,
            args.dataset or "LI-Mini",
            args.filter_usd_workers,
            args.sum_q2_workers,
            args.filter_q5_format_workers,
            args.prefetch,
            args.filter_q5_usd_workers,
            args.sg_mapper_workers,
            args.sg_linker_workers,
            args.sg_detector_workers,
            args.q4_filter_workers,
            args.q4_sum_workers,
            args.q4_joiner_workers,
            args.q4_aggregator_workers,
            args.q4_deduper_workers,
            clients=args.clients,
            q3_barrier_workers=args.q3_barrier_workers,
        )
        config_label = f"preset:{args.preset}"
    else:
        config_path = resolve_path(args.config)
        config = load_config(config_path)
        apply_cli_overrides(config, args, config_path)
        config_label = relative(config_path)

    output_file = resolve_path(
        args.output or config.get("compose", {}).get("output_file") or DEFAULT_COMPOSE
    )
    test_output_file = resolve_path(
        args.test_output
        or config.get("compose", {}).get("test_output_file")
        or DEFAULT_TEST_COMPOSE
    )

    generated = []
    if not args.skip_output:
        write_compose(config, output_file, expose_ports=bool_value(config, "rabbitmq_ports", True))
        generated.append(relative(output_file))
    if not args.skip_test_output:
        write_compose(config, test_output_file, expose_ports=bool_value(config, "rabbitmq_ports", False))
        generated.append(relative(test_output_file))
    print(f"generated {', '.join(generated)} from {config_label}")
    return 0


def parse_args():
    parser = argparse.ArgumentParser(description="Generate docker-compose files from a scenario config.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to the config YAML.")
    parser.add_argument("--output", help="Path for docker-compose.yaml.")
    parser.add_argument("--test-output", help="Path for docker-compose.test.yaml.")
    parser.add_argument("--preset", choices=("q1-test", "q2-test", "q3-test", "q5-test"), help="Use a built-in compose config preset.")
    parser.add_argument("--dataset", default=None, help="Dataset name override.")
    parser.add_argument("--filter-usd-workers", type=int, default=None, help="Override filter_usd worker count.")
    parser.add_argument("--sum-q2-workers", type=int, default=None, help="Override sum_q2 worker count.")
    parser.add_argument("--filter-q5-format-workers", type=int, default=None, help="Override filter_q5_format worker count.")
    parser.add_argument("--filter-q5-usd-workers", type=int, default=None, help="Override filter_q5_usd worker count.")
    parser.add_argument("--sg-mapper-workers", type=int, default=None, help="Override scatter-gather mapper worker count.")
    parser.add_argument("--sg-linker-workers", type=int, default=None, help="Override scatter-gather linker worker count.")
    parser.add_argument("--sg-detector-workers", type=int, default=None, help="Override scatter-gather detector worker count.")
    parser.add_argument("--q4-filter-workers", type=int, default=None, help="Override Q4 source prefilter worker count.")
    parser.add_argument("--q4-sum-workers", type=int, default=None, help="Override Q4 edge store worker count.")
    parser.add_argument("--q4-joiner-workers", type=int, default=None, help="Override Q4 block joiner worker count.")
    parser.add_argument("--q4-aggregator-workers", type=int, default=None, help="Override Q4 pair reducer worker count.")
    parser.add_argument("--q4-deduper-workers", type=int, default=None, help="Override Q4 account deduper worker count.")
    parser.add_argument("--q3-barrier-workers", type=int, default=None, help="Override q3_barrier worker count (sharded by client_id).")
    parser.add_argument("--prefetch", type=int, default=None, help="PREFETCH_COUNT for filter/sum services.")
    parser.add_argument("--clients", type=int, default=None, help="Number of client containers to spawn. Each gets a distinct client_id sharing the first configured dataset.")
    parser.add_argument("--chaos", action="store_true", default=None, help="Add the chaos monkey service that kills random workers.")
    parser.add_argument("--chaos-interval", type=int, default=None, help="Seconds between chaos monkey kills.")
    parser.add_argument("--skip-output", action="store_true", help="Do not write docker-compose.yaml.")
    parser.add_argument("--skip-test-output", action="store_true", help="Do not write docker-compose.test.yaml.")
    parser.set_defaults(skip_output=False, skip_test_output=False)
    args = parser.parse_args()
    if args.skip_output and args.skip_test_output:
        parser.error("at least one compose output must be enabled")
    return args


def preset_config(
    name: str,
    dataset: str,
    filter_usd_workers: int | None = None,
    sum_q2_workers: int | None = None,
    filter_q5_format_workers: int | None = None,
    prefetch: int | None = None,
    filter_q5_usd_workers: int | None = None,
    sg_mapper_workers: int | None = None,
    sg_linker_workers: int | None = None,
    sg_detector_workers: int | None = None,
    q4_filter_workers: int | None = None,
    q4_sum_workers: int | None = None,
    q4_joiner_workers: int | None = None,
    q4_aggregator_workers: int | None = None,
    q4_deduper_workers: int | None = None,
    clients: int | None = None,
    q3_barrier_workers: int | None = None,
) -> dict:
    if name not in ("q1-test", "q2-test", "q3-test", "q5-test"):
        raise ValueError(f"unknown preset: {name}")
    clients = clients or 1
    if clients < 1:
        raise ValueError("clients must be >= 1")

    client_accounts = [
        {
            "client_id": client_id,
            "accounts_file": f"data/datasets/{dataset}/{dataset}_accounts.csv",
            "transactions_file": f"data/datasets/{dataset}/{dataset}_Trans.csv",
        }
        for client_id in range(clients)
    ]

    config = {
        "compose": {
            "output_file": "docker-compose.yaml",
            "test_output_file": "docker-compose.test.yaml",
            "rabbitmq_ports": True,
        },
        "settings": {
            "logging_level": "INFO",
            "server_port": 5678,
            "chunk_max_bytes": 1048576,
            "result_line_max_bytes": 1048576,
            "connect_timeout_seconds": 30,
            "io_timeout_seconds": 3600,
            **({"filter_prefetch_count": prefetch} if prefetch is not None else {}),
        },
        "queries": {"q1-test": ["q1"], "q2-test": ["q2"], "q3-test": ["q3"], "q5-test": ["q5"]}[name],
        "workers": {
            "file_ingestors": 1,
            "filters": {
                "usd": filter_usd_workers if filter_usd_workers is not None else 1,
                "q1": 1,
                "date": 1,
                "q5_format": filter_q5_format_workers if filter_q5_format_workers is not None else 1,
                "q5_usd": filter_q5_usd_workers if filter_q5_usd_workers is not None else 1,
            },
            "sums": {
                "q2": sum_q2_workers if sum_q2_workers is not None else 1,
                "q3": 1,
            },
            "aggregators": {
                "q2": 1,
                "q3": 1,
                "q5": 1,
            },
            "joiners": {
                "q2": 1,
                "q3": 1,
                "q5": 1,
            },
            "q3_barrier": (
                q3_barrier_workers if q3_barrier_workers is not None else 1
            ),
            "scatter_gather": {
                "mappers": sg_mapper_workers if sg_mapper_workers is not None else 1,
                "linkers": sg_linker_workers if sg_linker_workers is not None else 1,
                "detectors": sg_detector_workers if sg_detector_workers is not None else 1,
                "min_intermediaries": 5,
            },
            "q4": {
                "filters": (
                    q4_filter_workers
                    if q4_filter_workers is not None
                    else 1
                ),
                "sums": (
                    q4_sum_workers
                    if q4_sum_workers is not None
                    else 1
                ),
                "joiners": (
                    q4_joiner_workers
                    if q4_joiner_workers is not None
                    else 1
                ),
                "aggregators": (
                    q4_aggregator_workers
                    if q4_aggregator_workers is not None
                    else 1
                ),
                "dedupers": (
                    q4_deduper_workers
                    if q4_deduper_workers is not None
                    else 1
                ),
            },
        },
        "clients": len(client_accounts),
        "client_accounts": client_accounts,
    }
    validate_config(config, Path(f"preset:{name}"))
    return config


def apply_cli_overrides(config: dict, args, path: Path) -> None:
    workers = config.setdefault("workers", {})
    filters = workers.setdefault("filters", {})
    sums = workers.setdefault("sums", {})
    scatter_gather = workers.setdefault("scatter_gather", {})
    q4 = workers.setdefault("q4", {})
    settings = config.setdefault("settings", {})

    if args.filter_usd_workers is not None:
        filters["usd"] = args.filter_usd_workers
    if args.sum_q2_workers is not None:
        sums["q2"] = args.sum_q2_workers
    if args.filter_q5_format_workers is not None:
        filters["q5_format"] = args.filter_q5_format_workers
    if args.filter_q5_usd_workers is not None:
        filters["q5_usd"] = args.filter_q5_usd_workers
    if args.sg_mapper_workers is not None:
        scatter_gather["mappers"] = args.sg_mapper_workers
    if args.sg_linker_workers is not None:
        scatter_gather["linkers"] = args.sg_linker_workers
    if args.sg_detector_workers is not None:
        scatter_gather["detectors"] = args.sg_detector_workers
    if args.q4_filter_workers is not None:
        q4["filters"] = args.q4_filter_workers
    if args.q4_sum_workers is not None:
        q4["sums"] = args.q4_sum_workers
    if args.q4_joiner_workers is not None:
        q4["joiners"] = args.q4_joiner_workers
    if args.q4_aggregator_workers is not None:
        q4["aggregators"] = args.q4_aggregator_workers
    if args.q4_deduper_workers is not None:
        q4["dedupers"] = args.q4_deduper_workers
    if args.q3_barrier_workers is not None:
        workers["q3_barrier"] = args.q3_barrier_workers
    if args.prefetch is not None:
        settings["filter_prefetch_count"] = args.prefetch
    if args.chaos:
        settings.setdefault("chaos", {})["enabled"] = True
    if args.chaos_interval is not None:
        settings.setdefault("chaos", {})["interval"] = args.chaos_interval
    if args.clients is not None:
        if args.clients < 1:
            raise ValueError("clients must be >= 1")
        accounts = config.get("client_accounts", [])
        if not accounts:
            raise ValueError(f"{path}: client_accounts must be a non-empty list")
        template = dict(accounts[0])
        config["client_accounts"] = [
            {**template, "client_id": client_id}
            for client_id in range(args.clients)
        ]
        config["clients"] = args.clients
    if args.dataset is not None:
        clients = int(config.get("clients", len(config.get("client_accounts", []))))
        config["client_accounts"] = [
            {
                "client_id": client_id,
                "accounts_file": (
                    f"data/datasets/{args.dataset}/"
                    f"{args.dataset}_accounts.csv"
                ),
                "transactions_file": (
                    f"data/datasets/{args.dataset}/"
                    f"{args.dataset}_Trans.csv"
                ),
            }
            for client_id in range(clients)
        ]

    validate_config(config, path)


def load_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}
    # Validation is deferred to apply_cli_overrides so that --dataset / --clients
    # overrides are applied before the file-existence checks run.
    return config


def validate_config(config: dict, path: Path) -> None:
    workers = config.get("workers", {})
    clients = int_value(config, "clients", default=None)
    client_accounts = config.get("client_accounts", [])
    if not isinstance(client_accounts, list) or not client_accounts:
        raise ValueError(f"{path}: client_accounts must be a non-empty list")
    if clients is None:
        config["clients"] = len(client_accounts)
    elif clients != len(client_accounts):
        raise ValueError(
            f"{path}: clients={clients} but client_accounts has {len(client_accounts)} entries"
        )

    positive_counts = {
        "workers.file_splitters": get_nested(
            workers,
            "file_splitters",
            get_nested(workers, "file_ingestors", 1),
        ),
        "workers.file_ingestors": get_nested(workers, "file_ingestors", 1),
        "workers.filters.usd": get_nested(workers, "filters.usd", 1),
        "workers.filters.q1": get_nested(workers, "filters.q1", 1),
        "workers.filters.date": get_nested(workers, "filters.date", 1),
        "workers.filters.q5_format": get_nested(workers, "filters.q5_format", 1),
        "workers.filters.q5_usd": get_nested(workers, "filters.q5_usd", 1),
        "workers.sums.q2": get_nested(workers, "sums.q2", 1),
        "workers.sums.q3": get_nested(workers, "sums.q3", 1),
        "workers.aggregators.q2": get_nested(workers, "aggregators.q2", 1),
        "workers.aggregators.q3": get_nested(workers, "aggregators.q3", 1),
        "workers.aggregators.q5": get_nested(workers, "aggregators.q5", 1),
        "workers.scatter_gather.mappers": get_nested(workers, "scatter_gather.mappers", 1),
        "workers.scatter_gather.linkers": get_nested(workers, "scatter_gather.linkers", 1),
        "workers.scatter_gather.detectors": get_nested(workers, "scatter_gather.detectors", 1),
        "workers.q4.filters": get_nested(workers, "q4.filters", 1),
        "workers.q4.sums": get_nested(workers, "q4.sums", 1),
        "workers.q4.joiners": get_nested(workers, "q4.joiners", 1),
        "workers.q4.aggregators": get_nested(workers, "q4.aggregators", 1),
        "workers.q4.dedupers": get_nested(workers, "q4.dedupers", 1),
    }
    for key, value in positive_counts.items():
        if int(value) <= 0:
            raise ValueError(f"{path}: {key} must be greater than 0")

    monitor = config.get("monitor", {})
    if monitor.get("enabled", False):
        monitor_count = int(monitor.get("count", DEFAULT_MONITOR_COUNT))
        if monitor_count < 1 or monitor_count > 255:
            raise ValueError(f"{path}: monitor.count must be in range [1, 255]")
        for key, default in (
            ("port", DEFAULT_MONITOR_PORT),
            ("election_port", DEFAULT_ELECTION_PORT),
        ):
            value = int(monitor.get(key, default))
            if value < 1 or value > 65535:
                raise ValueError(
                    f"{path}: monitor.{key} must be in range [1, 65535]"
                )
        for key, default in (
            ("check_interval", DEFAULT_MONITOR_CHECK_INTERVAL),
            ("election_timeout", DEFAULT_ELECTION_TIMEOUT),
            ("coordinator_timeout", DEFAULT_COORDINATOR_TIMEOUT),
        ):
            if float(monitor.get(key, default)) <= 0:
                raise ValueError(
                    f"{path}: monitor.{key} must be greater than 0"
                )
        startup_grace_period = float(
            monitor.get("startup_grace_period", DEFAULT_STARTUP_GRACE_PERIOD)
        )
        if startup_grace_period < 0:
            raise ValueError(
                f"{path}: monitor.startup_grace_period must be at least 0"
            )
        if int(monitor.get("max_missed", DEFAULT_MAX_MISSED)) <= 0:
            raise ValueError(f"{path}: monitor.max_missed must be greater than 0")

    joiners = workers.get("joiners", {})
    for key in ("q2", "q3"):
        value = int(joiners.get(key, 1))
        if value != 1:
            raise ValueError(f"{path}: workers.joiners.{key} must be 1 in the current topology")

    for account in client_accounts:
        accounts_file = require_relative_path(account, "accounts_file", path)
        transactions_file = require_relative_path(account, "transactions_file", path)
        if not (ROOT / accounts_file).exists():
            raise FileNotFoundError(f"{path}: accounts_file not found: {accounts_file}")
        if not (ROOT / transactions_file).exists():
            raise FileNotFoundError(f"{path}: transactions_file not found: {transactions_file}")


def write_compose(config: dict, path: Path, expose_ports: bool) -> None:
    compose = build_compose(config, expose_ports=expose_ports)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(compose, file, sort_keys=False)


def build_compose(config: dict, expose_ports: bool) -> dict:
    workers = config.get("workers", {})
    settings = config.get("settings", {})
    monitor_config = config.get("monitor", {})
    monitor_enabled = bool(monitor_config.get("enabled", False))
    enabled_queries = set(config.get("queries", ["q1", "q2", "q3", "q4", "q5"]))
    q1_enabled = "q1" in enabled_queries
    q2_enabled = "q2" in enabled_queries
    q3_enabled = "q3" in enabled_queries
    q4_enabled = "q4" in enabled_queries
    q5_enabled = "q5" in enabled_queries
    usd_enabled = q1_enabled or q2_enabled or q3_enabled or q4_enabled

    counts = {
        "file_splitters": int(
            get_nested(
                workers,
                "file_splitters",
                get_nested(workers, "file_ingestors", 1),
            )
        ),
        "file_ingestors": int(get_nested(workers, "file_ingestors", 1)),
        "filter_usd": int(get_nested(workers, "filters.usd", 1)),
        "filter_q1": int(get_nested(workers, "filters.q1", 1)),
        "filter_date": int(get_nested(workers, "filters.date", 1)),
        "filter_q5_format": int(get_nested(workers, "filters.q5_format", 1)),
        "filter_q5_usd": int(get_nested(workers, "filters.q5_usd", 1)),
        "sum_q2": int(get_nested(workers, "sums.q2", 1)),
        "sum_q3": int(get_nested(workers, "sums.q3", 1)),
        "aggregation_q2": int(get_nested(workers, "aggregators.q2", 1)),
        "aggregation_q3": int(get_nested(workers, "aggregators.q3", 1)),
        "aggregation_q5": int(get_nested(workers, "aggregators.q5", 1)),
        "sg_mapper": int(get_nested(workers, "scatter_gather.mappers", 1)),
        "sg_linker": int(get_nested(workers, "scatter_gather.linkers", 1)),
        "sg_detector": int(get_nested(workers, "scatter_gather.detectors", 1)),
        "q4_filter": int(get_nested(workers, "q4.filters", 1)),
        "q4_sum": int(get_nested(workers, "q4.sums", 1)),
        "q4_joiner": int(get_nested(workers, "q4.joiners", 1)),
        "q4_aggregator": int(get_nested(workers, "q4.aggregators", 1)),
        "q4_deduper": int(get_nested(workers, "q4.dedupers", 1)),
        "q3_barrier": int(workers.get("q3_barrier", 1)),
    }
    min_intermediaries = int(get_nested(workers, "scatter_gather.min_intermediaries", 5))

    services = {}
    services["rabbitmq"] = rabbitmq_service(expose_ports)
    services["gateway"] = gateway_service(
        counts["file_splitters"],
        settings,
        enabled_queries,
    )

    for index in range(counts["file_splitters"]):
        services[f"file_splitter_{index}"] = file_splitter_service(
            index, settings, q2_enabled=q2_enabled
        )

    file_ingestor_count = counts["file_ingestors"]
    named_volumes: dict[str, None] = {}
    for index in range(file_ingestor_count):
        vol_name = f"file_ingestor_{index}_state"
        named_volumes[vol_name] = None
        services[f"file_ingestor_{index}"] = file_ingestor_service(index, file_ingestor_count, settings, vol_name)

    filter_specs = []
    if usd_enabled:
        filter_specs.append(("USD", counts["filter_usd"], FILTER_USD_QUEUE))
    if q1_enabled:
        filter_specs.append(("Q1", counts["filter_q1"], FILTER_Q1_QUEUE))
    if q3_enabled or q4_enabled:
        filter_specs.append(("DATE", counts["filter_date"], FILTER_DATE_QUEUE))

    for configuration, count, input_queue in filter_specs:
        for index in range(count):
            services[f"filter_{configuration.lower()}_{index}"] = filter_service(
                configuration=configuration,
                index=index,
                amount=count,
                input_queue=input_queue,
                settings=settings,
                transaction_exchange=TRANSACTION_EXCHANGE if configuration == "USD" else None,
                enabled_queries=enabled_queries,
                q3_barrier_amount=counts["q3_barrier"],
                q4_filter_amount=counts["q4_filter"],
                sum_q2_amount=counts["sum_q2"],
            )

    if q5_enabled:
        for index in range(counts["filter_q5_format"]):
            services[f"filter_q5_format_{index}"] = filter_service(
                configuration="Q5",
                index=index,
                amount=counts["filter_q5_format"],
                input_queue=FILTER_Q5_FORMAT_QUEUE,
                settings=settings,
                transaction_exchange=TRANSACTION_EXCHANGE,
                enabled_queries=enabled_queries,
                q3_barrier_amount=counts["q3_barrier"],
                q4_filter_amount=counts["q4_filter"],
                sum_q2_amount=counts["sum_q2"],
            )

        services["rates_service"] = rates_service()

        for index in range(counts["filter_q5_usd"]):
            services[f"filter_q5_usd_{index}"] = filter_q5_usd_service(
                index=index,
                amount=counts["filter_q5_usd"],
                aggregation_amount=counts["aggregation_q5"],
                input_queue=FILTER_Q5_USD_QUEUE,
            )

    if q2_enabled:
        for index in range(counts["sum_q2"]):
            services[f"sum_q2_{index}"] = sum_service(
                configuration="Q2",
                index=index,
                amount=counts["sum_q2"],
                aggregation_amount=counts["aggregation_q2"],
                aggregation_prefix=AGGREGATION_Q2_PREFIX,
                input_queue=worker_queue_name(SUM_Q2_PREFIX, index),
                input_exchange=SUM_Q2_EXCHANGE,
                input_routing_prefix=SUM_Q2_PREFIX,
                settings=settings,
                sum_prefix=SUM_Q2_PREFIX,
            )

    if q3_enabled:
        for index in range(counts["sum_q3"]):
            services[f"sum_q3_{index}"] = sum_service(
                configuration="Q3",
                index=index,
                amount=counts["sum_q3"],
                aggregation_amount=counts["aggregation_q3"],
                aggregation_prefix=AGGREGATION_Q3_PREFIX,
                input_queue=SUM_Q3_QUEUE,
                input_exchange=None,
                input_routing_prefix=None,
                settings=settings,
                sum_prefix=SUM_Q3_PREFIX,
            )

    if q2_enabled:
        for index in range(counts["aggregation_q2"]):
            vol_name = f"aggregation_q2_{index}_state"
            named_volumes[vol_name] = None
            services[f"aggregation_q2_{index}"] = aggregator_service(
                configuration="Q2",
                index=index,
                amount=counts["aggregation_q2"],
                aggregation_prefix=AGGREGATION_Q2_PREFIX,
                output_queue=JOIN_Q2_QUEUE,
                sum_amount=counts["sum_q2"],
                sum_prefix=SUM_Q2_PREFIX,
                state_volume=vol_name,
            )

        services["join_q2"] = joiner_service(
            configuration="Q2",
            input_queue=JOIN_Q2_QUEUE,
            output_queue=Q2_ENRICH_QUEUE,
            aggregation_amount=counts["aggregation_q2"],
            aggregation_prefix=AGGREGATION_Q2_PREFIX,
            sum_amount=counts["sum_q2"],
            sum_prefix=SUM_Q2_PREFIX,
        )

        services["q2_bank_name_joiner"] = q2_bank_name_joiner_service(
            q2_input_queue=Q2_ENRICH_QUEUE,
            accounts_input_queue=ACCOUNTS_LINE_BATCH_QUEUE,
            output_queue=GATEWAY_Q2_QUEUE,
            settings=settings,
        )

    if q3_enabled:
        for index in range(counts["aggregation_q3"]):
            vol_name = f"aggregation_q3_{index}_state"
            named_volumes[vol_name] = None
            services[f"aggregation_q3_{index}"] = aggregator_service(
                configuration="Q3",
                index=index,
                amount=counts["aggregation_q3"],
                aggregation_prefix=AGGREGATION_Q3_PREFIX,
                output_queue=JOIN_Q3_QUEUE,
                sum_amount=counts["sum_q3"],
                sum_prefix=SUM_Q3_PREFIX,
                state_volume=vol_name,
            )

        services["join_q3"] = joiner_service(
            configuration="Q3",
            input_queue=JOIN_Q3_QUEUE,
            output_queue=JOIN_Q3_RESULTS_QUEUE,
            aggregation_amount=counts["aggregation_q3"],
            aggregation_prefix=AGGREGATION_Q3_PREFIX,
            sum_amount=counts["sum_q3"],
            sum_prefix=SUM_Q3_PREFIX,
            q3_barrier_amount=counts["q3_barrier"],
        )
        barrier_amount = counts["q3_barrier"]
        for index in range(barrier_amount):
            services[f"q3_barrier_{index}"] = q3_barrier_service(
                index=index,
                barrier_amount=barrier_amount,
                averages_queue=JOIN_Q3_RESULTS_QUEUE,
                candidates_queue=Q3_CANDIDATES_QUEUE,
                output_queue=GATEWAY_Q3_QUEUE,
            )

    if q5_enabled:
        for index in range(counts["aggregation_q5"]):
            vol_name = f"aggregation_q5_{index}_state"
            named_volumes[vol_name] = None
            services[f"aggregation_q5_{index}"] = aggregator_service(
                configuration="Q5",
                index=index,
                amount=counts["aggregation_q5"],
                aggregation_prefix=AGGREGATION_Q5_PREFIX,
                output_queue=JOIN_Q5_QUEUE,
                sum_amount=counts["filter_q5_usd"],
                sum_prefix="filter_q5_usd",
                state_volume=vol_name,
            )

        services["join_q5"] = joiner_service(
            configuration="Q5",
            input_queue=JOIN_Q5_QUEUE,
            output_queue=GATEWAY_Q5_QUEUE,
            aggregation_amount=counts["aggregation_q5"],
            aggregation_prefix=AGGREGATION_Q5_PREFIX,
            sum_amount=counts["filter_q5_usd"],
            sum_prefix="filter_q5_usd",
        )

    if q4_enabled:
        for index in range(counts["q4_filter"]):
            services[f"q4_filter_{index}"] = q4_filter_service(
                index=index,
                amount=counts["q4_filter"],
                sum_amount=counts["q4_sum"],
                settings=settings,
            )
        for index in range(counts["q4_sum"]):
            services[f"q4_sum_{index}"] = q4_sum_service(
                index=index,
                filter_amount=counts["q4_filter"],
                joiner_amount=counts["q4_joiner"],
                settings=settings,
            )
        for index in range(counts["q4_joiner"]):
            services[f"q4_joiner_{index}"] = q4_joiner_service(
                index=index,
                sum_amount=counts["q4_sum"],
                aggregator_amount=counts["q4_aggregator"],
                settings=settings,
            )
        for index in range(counts["q4_aggregator"]):
            services[f"q4_aggregator_{index}"] = q4_aggregator_service(
                index=index,
                joiner_amount=counts["q4_joiner"],
                deduper_amount=counts["q4_deduper"],
                settings=settings,
            )
        for index in range(counts["q4_deduper"]):
            services[f"q4_deduper_{index}"] = q4_deduper_service(
                index=index,
                aggregator_amount=counts["q4_aggregator"],
                deduper_amount=counts["q4_deduper"],
                settings=settings,
            )

    rabbitmq_service_names = [
        name for name in services if name != "rabbitmq"
    ]
    for name in rabbitmq_service_names:
        add_env_once(services[name], RABBITMQ_DURABLE_ENV)

    worker_service_names = [
        name
        for name in services
        if name not in {"rabbitmq", "gateway", "rates_service"}
    ]
    for name in worker_service_names:
        add_worker_state_volume(name, services[name], named_volumes)

    heartbeat_node_names = [
        *worker_service_names
    ]
    monitor_names = []
    if monitor_enabled:
        monitor_count = int(
            monitor_config.get("count", DEFAULT_MONITOR_COUNT)
        )
        monitor_names = [
            f"monitor_{monitor_id}"
            for monitor_id in range(1, monitor_count + 1)
        ]
        nodes_to_watch = [*heartbeat_node_names, *monitor_names]
        for monitor_id, name in enumerate(monitor_names, start=1):
            services[name] = monitor_service(
                monitor_id=monitor_id,
                monitor_count=monitor_count,
                nodes_to_watch=nodes_to_watch,
                monitor_config=monitor_config,
                settings=settings,
            )
        heartbeat_node_names.extend(monitor_names)

    client_dependencies = [
        name for name in services if name not in {"rabbitmq", "gateway"}
    ]
    client_dependencies.insert(0, "gateway")
    client_names = []
    for client_index, account in enumerate(config["client_accounts"]):
        client_id = int(account.get("client_id", client_index))
        name = f"client_{client_id}"
        client_names.append(name)
        services[name] = client_service(
            client_id=client_id,
            account=account,
            settings=settings,
            depends_on=client_dependencies,
        )

    monitor_hosts = ",".join(monitor_names)
    monitor_port = int(
        monitor_config.get("port", DEFAULT_MONITOR_PORT)
    )
    for name in heartbeat_node_names:
        service = services[name]
        service["container_name"] = name
        service.setdefault("environment", []).append(f"NODE_NAME={name}")
        if monitor_enabled:
            service["environment"].extend(
                [
                    f"MONITOR_HOSTS={monitor_hosts}",
                    f"MONITOR_PORT={monitor_port}",
                ]
            )

    if settings.get("chaos", {}).get("enabled", False):
        services["chaos_monkey"] = chaos_monkey_service(settings, client_names)

    compose: dict = {"services": services}
    if named_volumes:
        compose["volumes"] = named_volumes
    return compose


def rabbitmq_service(expose_ports: bool) -> dict:
    service = {
        "build": {"context": "./rabbitmq", "dockerfile": "Dockerfile"},
        "environment": ["RABBITMQ_LOG_LEVELS=error"],
        "healthcheck": {
            "interval": "5s",
            "retries": 10,
            "start_period": "20s",
            "test": "rabbitmq-diagnostics check_port_connectivity",
            "timeout": "3s",
        },
    }
    if expose_ports:
        service["ports"] = ["5672:5672", "15672:15672"]
    return service


def gateway_service(file_ingestor_count: int, settings: dict, enabled_queries: set[str]) -> dict:
    environment = [
        f"FILE_INGESTOR_EXCHANGE={FILE_INGESTOR_EXCHANGE}",
        f"FILE_INGESTOR_PARTITIONS={file_ingestor_count}",
        f"FILE_SPLITTER_QUEUE_PREFIX={FILE_SPLITTER_QUEUE_PREFIX}",
        f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"SERVER_HOST={SERVER_HOST}",
        f"SERVER_PORT={settings.get('server_port', SERVER_PORT)}",
    ]
    if "q1" not in enabled_queries:
        environment.append("GATEWAY_Q1_ENABLED=0")
    if "q2" in enabled_queries:
        environment.append(f"GATEWAY_Q2_QUEUE={GATEWAY_Q2_QUEUE}")
    if "q3" in enabled_queries:
        environment.append(f"GATEWAY_Q3_QUEUE={GATEWAY_Q3_QUEUE}")
    if "q4" in enabled_queries:
        environment.append(f"GATEWAY_Q4_QUEUE={GATEWAY_Q4_QUEUE}")
    if "q5" in enabled_queries:
        environment.append(f"GATEWAY_Q5_QUEUE={GATEWAY_Q5_QUEUE}")

    return base_service(
        "gateway/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def file_ingestor_service(index: int, total: int, settings: dict, state_volume: str) -> dict:
    return base_service(
        "workers/file_ingestor/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=[
            f"ID={index}",
            f"FILE_INGESTOR_AMOUNT={total}",
            f"FILE_INGESTOR_CONTROL_QUEUE_PREFIX=file_ingestor_control",
            f"FILE_INGESTOR_RESPONSE_QUEUE_PREFIX={FILE_INGESTOR_RESPONSE_QUEUE_PREFIX}",
            f"LINE_BATCH_INPUT_QUEUE={LINE_BATCH_QUEUE}",
            f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
            f"MOM_HOST={MOM_HOST}",
            "PYTHONUNBUFFERED=1",
            f"TRANSACTION_OUTPUT_EXCHANGE={TRANSACTION_EXCHANGE}",
            "STATE_DIR=/worker_state",
            "SNAPSHOT_INTERVAL=1000",
        ],
        volumes=[f"{state_volume}:/worker_state"],
    )


def file_splitter_service(index: int, settings: dict, q2_enabled: bool) -> dict:
    environment = [
        f"FILE_SPLITTER_INPUT_EXCHANGE={FILE_INGESTOR_EXCHANGE}",
        f"FILE_SPLITTER_QUEUE_PREFIX={FILE_SPLITTER_QUEUE_PREFIX}",
        f"ID={index}",
        f"LINE_BATCH_OUTPUT_QUEUE={LINE_BATCH_QUEUE}",
        f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
        f"MAX_BATCH_BYTES={settings.get('chunk_max_bytes', 65536)}",
        f"MAX_LINE_BYTES={settings.get('max_line_bytes', 16777216)}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
    ]
    if q2_enabled:
        environment.append(
            f"ACCOUNTS_LINE_BATCH_OUTPUT_QUEUE={ACCOUNTS_LINE_BATCH_QUEUE}"
        )

    return base_service(
        "workers/file_splitter/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def q2_bank_name_joiner_service(
    q2_input_queue: str,
    accounts_input_queue: str,
    output_queue: str,
    settings: dict,
) -> dict:
    return base_service(
        "workers/q2_bank_name_joiner/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=[
            f"ACCOUNTS_INPUT_QUEUE={accounts_input_queue}",
            "ID=0",
            f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
            f"MOM_HOST={MOM_HOST}",
            f"OUTPUT_QUEUE={output_queue}",
            "PYTHONUNBUFFERED=1",
            f"Q2_INPUT_QUEUE={q2_input_queue}",
        ],
    )


def filter_service(
    configuration: str,
    index: int,
    amount: int,
    input_queue: str,
    settings: dict,
    transaction_exchange: str | None = None,
    enabled_queries: set[str] | None = None,
    q3_barrier_amount: int = 1,
    q4_filter_amount: int = 1,
    sum_q2_amount: int = 1,
) -> dict:
    enabled_queries = enabled_queries or {"q1", "q2", "q3", "q4", "q5"}
    environment = [
        f"CONFIGURATION={configuration}",
        f"FILTER_AMOUNT={amount}",
        f"FILTER_DATE_QUEUE={FILTER_DATE_QUEUE}",
        f"FILTER_PREFIX={FILTER_PREFIX}",
        f"FILTER_Q1_QUEUE={FILTER_Q1_QUEUE}",
        f"FILTER_Q3_QUEUE={FILTER_Q3_QUEUE}",
        f"FILTER_Q5_USD_QUEUE={FILTER_Q5_USD_QUEUE}",
        f"GATEWAY_QUEUE={GATEWAY_Q1_QUEUE}",
        f"ID={index}",
        f"INPUT_QUEUE={input_queue}",
        f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"Q3_BARRIER_AMOUNT={q3_barrier_amount}",
        f"Q3_CANDIDATES_QUEUE={Q3_CANDIDATES_QUEUE}",
        f"SCATTER_GATHER_MAPPER_QUEUE={SG_MAPPER_QUEUE}",
        f"SUM_PREFIX={SUM_Q3_PREFIX}",
        f"SUM_Q3_QUEUE={SUM_Q3_QUEUE}",
        f"USD_ENABLE_Q1={int('q1' in enabled_queries)}",
        f"USD_ENABLE_Q2={int('q2' in enabled_queries)}",
        f"USD_ENABLE_DATE={int(('q3' in enabled_queries) or ('q4' in enabled_queries))}",
        f"DATE_ENABLE_Q3={int('q3' in enabled_queries)}",
        f"DATE_ENABLE_Q4={int('q4' in enabled_queries)}",
    ]
    if "q4" in enabled_queries:
        environment.extend([
            f"Q4_FILTER_AMOUNT={q4_filter_amount}",
            f"Q4_FILTER_INPUT_EXCHANGE={Q4_FILTER_INPUT_EXCHANGE}",
            f"Q4_FILTER_INPUT_ROUTING_PREFIX={Q4_FILTER_ROUTING_PREFIX}",
        ])
    if "q2" in enabled_queries:
        environment.extend([
            f"SUM_Q2_AMOUNT={sum_q2_amount}",
            f"SUM_Q2_EXCHANGE={SUM_Q2_EXCHANGE}",
            f"SUM_Q2_ROUTING_PREFIX={SUM_Q2_PREFIX}",
        ])
    # Sharded mode: el filter_date publica candidates al exchange con routing
    # key por client_id en lugar de la queue compartida.
    if q3_barrier_amount > 1:
        environment.extend([
            f"Q3_CANDIDATES_EXCHANGE={Q3_CANDIDATES_EXCHANGE}",
            f"Q3_CANDIDATES_ROUTING_PREFIX={Q3_CANDIDATES_ROUTING_PREFIX}",
        ])
    if transaction_exchange:
        environment.append(f"TRANSACTION_EXCHANGE={transaction_exchange}")
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/filter/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def rates_service() -> dict:
    return {
        "build": {"context": ".", "dockerfile": "src/rates_service/Dockerfile"},
        "depends_on": depends_on_rabbitmq(),
        "environment": [
            f"RABBIT_HOST={MOM_HOST}",
            "RATES_REFERENCE_OVERLAY=q5",
            "PYTHONUNBUFFERED=1",
            *observability_env(),
        ],
        "volumes": ["./data/rates:/data/rates:rw"],
    }


def filter_q5_usd_service(
    index: int, amount: int, aggregation_amount: int, input_queue: str
) -> dict:
    return base_service(
        "workers/filter_q5_usd/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=[
            f"AGGREGATION_AMOUNT={aggregation_amount}",
            f"AGGREGATION_PREFIX={AGGREGATION_Q5_PREFIX}",
            f"FILTER_Q5_USD_AMOUNT={amount}",
            f"ID={index}",
            f"INPUT_QUEUE={input_queue}",
            f"MOM_HOST={MOM_HOST}",
            "PYTHONUNBUFFERED=1",
            f"RATES_REQUEST_QUEUE={RATES_REQUEST_QUEUE}",
            "Q5_START_DATE=2022-09-01",
            "Q5_END_DATE=2022-09-05",
        ],
    )


def sum_service(
    configuration: str,
    index: int,
    amount: int,
    aggregation_amount: int,
    aggregation_prefix: str,
    input_queue: str,
    input_exchange: str | None,
    input_routing_prefix: str | None,
    settings: dict,
    sum_prefix: str,
) -> dict:
    environment = [
        f"AGGREGATION_AMOUNT={aggregation_amount}",
        f"AGGREGATION_PREFIX={aggregation_prefix}",
        f"CONFIGURATION={configuration}",
        f"ID={index}",
        f"INPUT_QUEUE={input_queue}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"SUM_AMOUNT={amount}",
        f"SUM_PREFIX={sum_prefix}",
    ]
    if input_exchange is not None:
        environment.append(f"INPUT_EXCHANGE={input_exchange}")
    if input_routing_prefix is not None:
        environment.append(f"INPUT_ROUTING_PREFIX={input_routing_prefix}")
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/sum/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def aggregator_service(
    configuration: str,
    index: int,
    amount: int,
    aggregation_prefix: str,
    output_queue: str,
    sum_amount: int,
    sum_prefix: str,
    state_volume: str,
) -> dict:
    return base_service(
        "workers/aggregator/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=[
            f"AGGREGATION_AMOUNT={amount}",
            f"AGGREGATION_PREFIX={aggregation_prefix}",
            f"CONFIGURATION={configuration}",
            f"ID={index}",
            f"MOM_HOST={MOM_HOST}",
            f"OUTPUT_QUEUE={output_queue}",
            "PYTHONUNBUFFERED=1",
            f"SUM_AMOUNT={sum_amount}",
            f"SUM_PREFIX={sum_prefix}",
            "STATE_DIR=/worker_state",
            "SNAPSHOT_INTERVAL=1000",
        ],
        volumes=[f"{state_volume}:/worker_state"],
    )


def joiner_service(
    configuration: str,
    input_queue: str,
    output_queue: str,
    aggregation_amount: int,
    aggregation_prefix: str,
    sum_amount: int,
    sum_prefix: str,
    q3_barrier_amount: int = 1,
) -> dict:
    environment = [
        f"AGGREGATION_AMOUNT={aggregation_amount}",
        f"AGGREGATION_PREFIX={aggregation_prefix}",
        f"CONFIGURATION={configuration}",
        "ID=0",
        f"INPUT_QUEUE={input_queue}",
        f"MOM_HOST={MOM_HOST}",
        f"OUTPUT_QUEUE={output_queue}",
        "PYTHONUNBUFFERED=1",
        f"SUM_AMOUNT={sum_amount}",
        f"SUM_PREFIX={sum_prefix}",
    ]
    # Sharded Q3: el joiner enruta averages por client_id al barrier shard.
    if configuration == "Q3" and q3_barrier_amount > 1:
        environment.extend([
            f"Q3_BARRIER_AMOUNT={q3_barrier_amount}",
            f"Q3_AVERAGES_EXCHANGE={Q3_AVERAGES_EXCHANGE}",
            f"Q3_AVERAGES_ROUTING_PREFIX={Q3_AVERAGES_ROUTING_PREFIX}",
        ])
    return base_service(
        "workers/joiner/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def q3_barrier_service(
    index: int,
    barrier_amount: int,
    averages_queue: str,
    candidates_queue: str,
    output_queue: str,
) -> dict:
    environment = [
        f"ID={index}",
        f"GATEWAY_Q3_QUEUE={output_queue}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"Q3_AVERAGES_QUEUE={averages_queue}",
        f"Q3_CANDIDATES_QUEUE={candidates_queue}",
        f"Q3_BARRIER_AMOUNT={barrier_amount}",
        "Q3_THRESHOLD_DIVISOR=100",
    ]
    # Sharded mode: exponer los exchanges y prefijos de routing key. El barrier
    # creará su queue bindeada al routing key "{prefix}_{ID}".
    if barrier_amount > 1:
        environment.extend([
            f"Q3_AVERAGES_EXCHANGE={Q3_AVERAGES_EXCHANGE}",
            f"Q3_CANDIDATES_EXCHANGE={Q3_CANDIDATES_EXCHANGE}",
            f"Q3_AVERAGES_ROUTING_PREFIX={Q3_AVERAGES_ROUTING_PREFIX}",
            f"Q3_CANDIDATES_ROUTING_PREFIX={Q3_CANDIDATES_ROUTING_PREFIX}",
        ])
    return base_service(
        "workers/q3_barrier/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def q4_filter_service(
    index: int,
    amount: int,
    sum_amount: int,
    settings: dict,
) -> dict:
    environment = [
        f"ID={index}",
        f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"Q4_FILTER_AMOUNT={amount}",
        f"Q4_FILTER_INPUT_EXCHANGE={Q4_FILTER_INPUT_EXCHANGE}",
        f"Q4_FILTER_INPUT_ROUTING_PREFIX={Q4_FILTER_ROUTING_PREFIX}",
        f"Q4_FILTER_PREFIX={Q4_FILTER_ROUTING_PREFIX}",
        f"Q4_SUM_EXCHANGE={Q4_SUM_EXCHANGE}",
        f"Q4_SUM_AMOUNT={sum_amount}",
        f"Q4_SUM_ROUTING_PREFIX={Q4_SUM_ROUTING_PREFIX}",
    ]
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/scatter_gather/q4_filter/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def q4_sum_service(
    index: int,
    filter_amount: int,
    joiner_amount: int,
    settings: dict,
) -> dict:
    environment = [
        f"ID={index}",
        f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"Q4_SUM_EXCHANGE={Q4_SUM_EXCHANGE}",
        f"Q4_SUM_ROUTING_PREFIX={Q4_SUM_ROUTING_PREFIX}",
        f"Q4_FILTER_AMOUNT={filter_amount}",
        f"Q4_JOINER_EXCHANGE={Q4_JOINER_EXCHANGE}",
        f"Q4_JOINER_AMOUNT={joiner_amount}",
        f"Q4_JOINER_ROUTING_PREFIX={Q4_JOINER_ROUTING_PREFIX}",
    ]
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/scatter_gather/q4_sum/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def q4_joiner_service(
    index: int,
    sum_amount: int,
    aggregator_amount: int,
    settings: dict,
) -> dict:
    environment = [
        f"ID={index}",
        f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"Q4_JOINER_EXCHANGE={Q4_JOINER_EXCHANGE}",
        f"Q4_JOINER_ROUTING_PREFIX={Q4_JOINER_ROUTING_PREFIX}",
        f"Q4_SUM_AMOUNT={sum_amount}",
        f"Q4_AGGREGATOR_EXCHANGE={Q4_AGGREGATOR_EXCHANGE}",
        f"Q4_AGGREGATOR_AMOUNT={aggregator_amount}",
        f"Q4_AGGREGATOR_ROUTING_PREFIX={Q4_AGGREGATOR_ROUTING_PREFIX}",
    ]
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/scatter_gather/q4_joiner/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def q4_aggregator_service(
    index: int,
    joiner_amount: int,
    deduper_amount: int,
    settings: dict,
) -> dict:
    environment = [
        f"ID={index}",
        f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"Q4_AGGREGATOR_EXCHANGE={Q4_AGGREGATOR_EXCHANGE}",
        f"Q4_AGGREGATOR_ROUTING_PREFIX={Q4_AGGREGATOR_ROUTING_PREFIX}",
        f"Q4_JOINER_AMOUNT={joiner_amount}",
        f"Q4_DEDUPER_EXCHANGE={Q4_DEDUPER_EXCHANGE}",
        f"Q4_DEDUPER_AMOUNT={deduper_amount}",
        f"Q4_DEDUPER_ROUTING_PREFIX={Q4_DEDUPER_ROUTING_PREFIX}",
    ]
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/scatter_gather/q4_aggregator/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def q4_deduper_service(
    index: int,
    aggregator_amount: int,
    deduper_amount: int,
    settings: dict,
) -> dict:
    environment = [
        f"ID={index}",
        f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"Q4_DEDUPER_EXCHANGE={Q4_DEDUPER_EXCHANGE}",
        f"Q4_DEDUPER_ROUTING_PREFIX={Q4_DEDUPER_ROUTING_PREFIX}",
        f"Q4_AGGREGATOR_AMOUNT={aggregator_amount}",
        f"Q4_DEDUPER_AMOUNT={deduper_amount}",
        f"Q4_DEDUPER_RESPONSE_QUEUE_PREFIX={Q4_DEDUPER_RESPONSE_QUEUE_PREFIX}",
        f"GATEWAY_Q4_QUEUE={GATEWAY_Q4_QUEUE}",
    ]
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/scatter_gather/q4_deduper/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def scatter_detector_service(
    index: int,
    detector_amount: int,
    linker_amount: int,
    min_intermediaries: int,
    settings: dict,
) -> dict:
    environment = [
        f"GATEWAY_Q4_QUEUE={GATEWAY_Q4_QUEUE}",
        f"ID={index}",
        f"MIN_INTERMEDIARIES={min_intermediaries}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"SG_DETECTOR_AMOUNT={detector_amount}",
        f"SG_DETECTOR_EXCHANGE={SG_DETECTOR_EXCHANGE}",
        f"SG_LINKER_AMOUNT={linker_amount}",
    ]
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/scatter_gather/detector/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def scatter_linker_service(index: int, detector_amount: int, settings: dict) -> dict:
    environment = [
        f"ID={index}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"SG_DETECTOR_AMOUNT={detector_amount}",
        f"SG_DETECTOR_EXCHANGE={SG_DETECTOR_EXCHANGE}",
        f"SG_LINKER_EXCHANGE={SG_LINKER_EXCHANGE}",
    ]
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/scatter_gather/linker/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def scatter_mapper_service(
    index: int, mapper_amount: int, linker_amount: int, settings: dict
) -> dict:
    environment = [
        f"ID={index}",
        f"INPUT_QUEUE={SG_MAPPER_QUEUE}",
        f"MOM_HOST={MOM_HOST}",
        "PYTHONUNBUFFERED=1",
        f"SG_LINKER_AMOUNT={linker_amount}",
        f"SG_LINKER_EXCHANGE={SG_LINKER_EXCHANGE}",
        f"SG_MAPPER_AMOUNT={mapper_amount}",
    ]
    prefetch = settings.get("filter_prefetch_count")
    if prefetch is not None:
        environment.append(f"PREFETCH_COUNT={prefetch}")

    return base_service(
        "workers/scatter_gather/mapper/Dockerfile",
        depends_on=depends_on_rabbitmq(),
        environment=environment,
    )


def client_service(client_id: int, account: dict, settings: dict, depends_on: list[str]) -> dict:
    accounts_file = require_relative_path(account, "accounts_file", DEFAULT_CONFIG)
    transactions_file = require_relative_path(account, "transactions_file", DEFAULT_CONFIG)
    mount_dir = common_mount_dir(accounts_file, transactions_file)
    accounts_inside = os.path.relpath(str(accounts_file), str(mount_dir))
    transactions_inside = os.path.relpath(str(transactions_file), str(mount_dir))
    return base_service(
        "client/Dockerfile",
        depends_on=depends_on,
        environment=[
            f"ACCOUNTS_FILE={accounts_inside}",
            f"TRANSACTIONS_FILE={transactions_inside}",
            f"CHUNK_MAX_BYTES={settings.get('chunk_max_bytes', 65536)}",
            f"CLIENT_ID={client_id}",
            f"CONNECT_TIMEOUT_SECONDS={settings.get('connect_timeout_seconds', 30)}",
            "DATA_DIR=/data/input",
            "OUTPUT_DIR=/data/output",
            f"IO_TIMEOUT_SECONDS={settings.get('io_timeout_seconds', 3600)}",
            f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
            "PYTHONUNBUFFERED=1",
            f"RESULT_LINE_MAX_BYTES={settings.get('result_line_max_bytes', 1048576)}",
            f"SERVER_HOST={SERVER_HOST}",
            f"SERVER_PORT={settings.get('server_port', SERVER_PORT)}",
        ],
        volumes=[
            f"./{mount_dir.as_posix()}:/data/input:ro",
            "./data/output:/data/output:rw",
        ],
    )


def chaos_monkey_service(settings: dict, client_names: list[str]) -> dict:
    chaos = settings.get("chaos", {})
    return {
        "build": {"context": "./chaos_monkey", "dockerfile": "Dockerfile"},
        "environment": [
            "PYTHONUNBUFFERED=1",
            "CHAOS_ENABLED=true",
            f"CHAOS_INTERVAL={int(chaos.get('interval', 30))}",
        ],
        "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
        "depends_on": {name: {"condition": "service_started"} for name in client_names},
    }


def monitor_service(
    monitor_id: int,
    monitor_count: int,
    nodes_to_watch: list[str],
    monitor_config: dict,
    settings: dict,
) -> dict:
    monitor_port = int(
        monitor_config.get("port", DEFAULT_MONITOR_PORT)
    )
    election_port = int(
        monitor_config.get("election_port", DEFAULT_ELECTION_PORT)
    )
    return {
        "build": {
            "context": "./src/",
            "dockerfile": "monitor/Dockerfile",
        },
        "environment": [
            f"MONITOR_ID={monitor_id}",
            f"MONITOR_COUNT={monitor_count}",
            f"MONITOR_PORT={monitor_port}",
            f"ELECTION_PORT={election_port}",
            (
                "MONITOR_CHECK_INTERVAL="
                f"{monitor_config.get('check_interval', DEFAULT_MONITOR_CHECK_INTERVAL)}"
            ),
            (
                "MAX_MISSED="
                f"{monitor_config.get('max_missed', DEFAULT_MAX_MISSED)}"
            ),
            (
                "ELECTION_TIMEOUT="
                f"{monitor_config.get('election_timeout', DEFAULT_ELECTION_TIMEOUT)}"
            ),
            (
                "COORDINATOR_TIMEOUT="
                f"{monitor_config.get('coordinator_timeout', DEFAULT_COORDINATOR_TIMEOUT)}"
            ),
            (
                "STARTUP_GRACE_PERIOD="
                f"{monitor_config.get('startup_grace_period', DEFAULT_STARTUP_GRACE_PERIOD)}"
            ),
            "MONITOR_STATE_PATH=/data/monitor/epoch.json",
            f"NODES_TO_WATCH={','.join(nodes_to_watch)}",
            "PINNED_CONTAINER_NAMES=true",
            f"LOGGING_LEVEL={settings.get('logging_level', 'INFO')}",
            "PYTHONUNBUFFERED=1",
        ],
        "expose": [
            f"{monitor_port}/udp",
            str(election_port),
        ],
        "volumes": [
            "/var/run/docker.sock:/var/run/docker.sock",
            f"./data/monitor/monitor_{monitor_id}:/data/monitor:rw",
        ],
    }


def base_service(dockerfile: str, depends_on, environment: list[str], volumes: list[str] | None = None) -> dict:
    service = {
        "build": {"context": "./src/", "dockerfile": dockerfile},
        "depends_on": depends_on,
        "environment": [*environment, *observability_env()],
    }
    if volumes:
        service["volumes"] = volumes
    return service


def add_worker_state_volume(
    service_name: str,
    service: dict,
    named_volumes: dict[str, None],
) -> None:
    volume_name = worker_state_volume_name(service_name)
    named_volumes[volume_name] = None
    add_env_once(service, f"STATE_DIR={WORKER_STATE_DIR}")
    add_env_once(service, f"SNAPSHOT_INTERVAL={DEFAULT_SNAPSHOT_INTERVAL}")
    add_volume_once(service, f"{volume_name}:{WORKER_STATE_DIR}")


def worker_state_volume_name(service_name: str) -> str:
    return f"{service_name}_state"


def worker_queue_name(prefix: str, index: int) -> str:
    return f"{prefix}_{int(index)}"


def add_env_once(service: dict, item: str) -> None:
    environment = service.setdefault("environment", [])
    key = item.split("=", 1)[0]
    if any(entry.split("=", 1)[0] == key for entry in environment):
        return
    environment.append(item)


def add_volume_once(service: dict, item: str) -> None:
    volumes = service.setdefault("volumes", [])
    if item not in volumes:
        volumes.append(item)


def observability_env() -> list[str]:
    return [
        f"{name}={os.getenv(name, default)}"
        for name, default in OBSERVABILITY_DEFAULTS.items()
    ]


def depends_on_rabbitmq() -> dict:
    return {"rabbitmq": {"condition": "service_healthy"}}


def int_value(config: dict, key: str, default=None):
    value = config.get(key, default)
    if value is None:
        return None
    return int(value)


def bool_value(config: dict, key: str, default: bool) -> bool:
    return bool(config.get("compose", {}).get(key, default))


def get_nested(config: dict, path: str, default):
    node = config
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node


def require_relative_path(account: dict, key: str, source: Path) -> Path:
    value = account.get(key)
    if not value:
        raise ValueError(f"{source}: client account entry missing {key}")
    path = Path(value)
    if path.is_absolute():
        raise ValueError(f"{source}: {key} must be relative to the repository root")
    return path


def common_mount_dir(path_a: Path, path_b: Path) -> Path:
    parent = Path(os.path.commonpath([path_a.parent, path_b.parent]))
    if parent == Path("."):
        raise ValueError("client input files must share a non-root parent directory")
    return parent


def resolve_path(path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    return ROOT / path


def relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
