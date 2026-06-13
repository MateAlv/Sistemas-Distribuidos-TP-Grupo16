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
    heartbeat_target_hosts: tuple[str, ...]
    logging_level: str


def main() -> int:
    try:
        config = load_config()
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.error("monitor_config | result=error | error=%s", exc)
        return 2

    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=getattr(logging, config.logging_level.upper(), logging.INFO),
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    monitor = build_monitor(config)
    heartbeat = build_heartbeat_sender(config)

    def _shutdown(signum, _frame) -> None:
        logging.info("monitor_signal | signal=%s", signum)
        heartbeat.stop()
        monitor.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

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
    monitor_id = _parse_int(env, "MONITOR_ID")
    monitor_count = _parse_int(env, "MONITOR_COUNT")
    if not 1 <= monitor_count <= 255:
        raise ValueError("MONITOR_COUNT must be in range [1, 255]")
    if not 1 <= monitor_id <= monitor_count:
        raise ValueError("MONITOR_ID must be in range [1, MONITOR_COUNT]")

    monitor_port = _parse_port(env, "MONITOR_PORT", DEFAULT_MONITOR_PORT)
    election_port = _parse_port(env, "ELECTION_PORT", DEFAULT_ELECTION_PORT)
    check_interval = _parse_positive_float(env, "MONITOR_CHECK_INTERVAL", DEFAULT_CHECK_INTERVAL)
    election_timeout = _parse_positive_float(env, "ELECTION_TIMEOUT", DEFAULT_ELECTION_TIMEOUT)
    coordinator_timeout = _parse_positive_float(env, "COORDINATOR_TIMEOUT", DEFAULT_COORDINATOR_TIMEOUT)
    max_missed = _parse_int(env, "MAX_MISSED", DEFAULT_MAX_MISSED)
    if max_missed < 1:
        raise ValueError("MAX_MISSED must be greater than 0")

    raw_nodes = env.get("NODES_TO_WATCH")
    if raw_nodes is None:
        raise ValueError("NODES_TO_WATCH is required")
    nodes_to_watch = tuple(
        dict.fromkeys(n.strip() for n in raw_nodes.split(",") if n.strip())
    )
    if not nodes_to_watch:
        raise ValueError("NODES_TO_WATCH must contain at least one node")

    raw_hosts = env.get("MONITOR_HOSTS", DEFAULT_HEARTBEAT_TARGET_HOST)
    heartbeat_target_hosts = tuple(
        dict.fromkeys(h.strip() for h in raw_hosts.split(",") if h.strip())
    ) or (DEFAULT_HEARTBEAT_TARGET_HOST,)

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
        heartbeat_target_hosts=heartbeat_target_hosts,
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
        hosts=config.heartbeat_target_hosts,
        port=config.monitor_port,
    )


def _parse_int(env: Mapping[str, str], name: str, default: int | None = None) -> int:
    raw = env.get(name, None if default is None else str(default))
    if raw is None:
        raise ValueError(f"{name} is required")
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer")


def _parse_port(env: Mapping[str, str], name: str, default: int) -> int:
    value = _parse_int(env, name, default)
    if not 1 <= value <= 65535:
        raise ValueError(f"{name} must be in range [1, 65535]")
    return value


def _parse_positive_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name, str(default))
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number")
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
