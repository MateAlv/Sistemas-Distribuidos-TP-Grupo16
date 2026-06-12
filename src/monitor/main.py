import logging
import os
import signal
from collections.abc import Mapping
from dataclasses import dataclass

from common.heartbeat import HeartbeatSender
from monitor.election import ElectionHandler
from monitor.heartbeat import HeartbeatReceiver
from monitor.monitor import (
    DEFAULT_CHECK_INTERVAL,
    DEFAULT_COORDINATOR_TIMEOUT,
    DEFAULT_MAX_MISSED,
    Monitor,
)


DEFAULT_MONITOR_HOST = "0.0.0.0"
DEFAULT_MONITOR_PORT = 9000
DEFAULT_ELECTION_HOST = "0.0.0.0"
DEFAULT_ELECTION_PORT = 9001
DEFAULT_ELECTION_TIMEOUT = 5.0
DEFAULT_HEARTBEAT_TARGET_HOST = "monitor"
DEFAULT_LOGGING_LEVEL = "INFO"
MIN_TCP_PORT = 1
MAX_TCP_PORT = 65535


@dataclass(frozen=True)
class MonitorConfig:
    monitor_id: int
    monitor_count: int
    monitor_host: str
    monitor_port: int
    election_host: str
    election_port: int
    election_timeout: float
    coordinator_timeout: float
    check_interval: float
    max_missed: int
    nodes_to_watch: tuple[str, ...]
    heartbeat_target_host: str
    logging_level: str


def main() -> int:
    try:
        config = load_config()
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.error("monitor_config | result=error | error=%s", exc)
        return 2

    initialize_log(config.logging_level)
    monitor = build_monitor(config)
    heartbeat = build_heartbeat_sender(config)
    install_signal_handlers(monitor, heartbeat)

    logging.info(
        "monitor_config | result=ok | monitor_id=%s | monitor_count=%s | "
        "monitor_port=%s | election_port=%s | nodes_to_watch=%s",
        config.monitor_id,
        config.monitor_count,
        config.monitor_port,
        config.election_port,
        len(config.nodes_to_watch),
    )

    heartbeat.start()
    try:
        monitor.run()
    finally:
        heartbeat.stop()
        monitor.stop()
    return 0


def load_config(env: Mapping[str, str] = os.environ) -> MonitorConfig:
    monitor_id = _required_int(env, "MONITOR_ID")
    monitor_count = _required_int(env, "MONITOR_COUNT")
    if monitor_count < 1 or monitor_count > 255:
        raise ValueError("MONITOR_COUNT must be in range [1, 255]")
    if not 1 <= monitor_id <= monitor_count:
        raise ValueError("MONITOR_ID must be in range [1, MONITOR_COUNT]")

    monitor_port = _port(env, "MONITOR_PORT", DEFAULT_MONITOR_PORT)
    election_port = _port(env, "ELECTION_PORT", DEFAULT_ELECTION_PORT)
    check_interval = _positive_float(
        env,
        "MONITOR_CHECK_INTERVAL",
        DEFAULT_CHECK_INTERVAL,
    )
    election_timeout = _positive_float(
        env,
        "ELECTION_TIMEOUT",
        DEFAULT_ELECTION_TIMEOUT,
    )
    coordinator_timeout = _positive_float(
        env,
        "COORDINATOR_TIMEOUT",
        DEFAULT_COORDINATOR_TIMEOUT,
    )
    max_missed = _positive_int(env, "MAX_MISSED", DEFAULT_MAX_MISSED)

    raw_nodes = env.get("NODES_TO_WATCH")
    if raw_nodes is None:
        raise ValueError("NODES_TO_WATCH is required")
    nodes_to_watch = tuple(
        dict.fromkeys(
            node.strip()
            for node in raw_nodes.split(",")
            if node.strip()
        )
    )
    if not nodes_to_watch:
        raise ValueError("NODES_TO_WATCH must contain at least one node")

    return MonitorConfig(
        monitor_id=monitor_id,
        monitor_count=monitor_count,
        monitor_host=env.get("MONITOR_BIND_HOST", DEFAULT_MONITOR_HOST),
        monitor_port=monitor_port,
        election_host=env.get("ELECTION_BIND_HOST", DEFAULT_ELECTION_HOST),
        election_port=election_port,
        election_timeout=election_timeout,
        coordinator_timeout=coordinator_timeout,
        check_interval=check_interval,
        max_missed=max_missed,
        nodes_to_watch=nodes_to_watch,
        heartbeat_target_host=env.get(
            "MONITOR_HOST",
            DEFAULT_HEARTBEAT_TARGET_HOST,
        ),
        logging_level=env.get("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL),
    )


def build_monitor(config: MonitorConfig) -> Monitor:
    election_handler = ElectionHandler(
        monitor_id=config.monitor_id,
        monitor_count=config.monitor_count,
        host=config.election_host,
        port=config.election_port,
        election_timeout=config.election_timeout,
    )
    heartbeat_receiver = HeartbeatReceiver(
        host=config.monitor_host,
        port=config.monitor_port,
    )
    return Monitor(
        election_handler=election_handler,
        heartbeat_receiver=heartbeat_receiver,
        nodes_to_watch=config.nodes_to_watch,
        check_interval=config.check_interval,
        max_missed=config.max_missed,
        coordinator_timeout=config.coordinator_timeout,
    )


def build_heartbeat_sender(config: MonitorConfig) -> HeartbeatSender:
    return HeartbeatSender(
        node_id=f"monitor_{config.monitor_id}",
        host=config.heartbeat_target_host,
        port=config.monitor_port,
    )


def install_signal_handlers(
    monitor: Monitor,
    heartbeat: HeartbeatSender,
) -> None:
    def shutdown(signum, _frame) -> None:
        logging.info("monitor_signal | signal=%s", signum)
        heartbeat.stop()
        monitor.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)


def initialize_log(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _required_int(env: Mapping[str, str], name: str) -> int:
    value = env.get(name)
    if value is None:
        raise ValueError(f"{name} is required")
    return _parse_int(name, value)


def _positive_int(
    env: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    value = _parse_int(name, env.get(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be greater than 0")
    return value


def _positive_float(
    env: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    raw_value = env.get(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return value


def _port(
    env: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    value = _parse_int(name, env.get(name, str(default)))
    if value < MIN_TCP_PORT or value > MAX_TCP_PORT:
        raise ValueError(
            f"{name} must be in range [{MIN_TCP_PORT}, {MAX_TCP_PORT}]"
        )
    return value


def _parse_int(name: str, value: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


if __name__ == "__main__":
    raise SystemExit(main())
