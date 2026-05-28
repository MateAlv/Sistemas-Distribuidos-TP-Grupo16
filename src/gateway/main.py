import logging
import os
import signal

from gateway import Gateway, GatewayConfig


DEFAULT_SERVER_HOST = "gateway"
DEFAULT_SERVER_PORT = 5678
DEFAULT_MOM_HOST = "rabbitmq"
DEFAULT_FILE_INGESTOR_EXCHANGE = "file_ingestor_exchange"
DEFAULT_FILE_INGESTOR_PARTITIONS = 1
DEFAULT_FILE_SPLITTER_QUEUE_PREFIX = "file_splitter"
DEFAULT_LOGGING_LEVEL = "INFO"
MIN_TCP_PORT = 1
MAX_TCP_PORT = 65535


def main() -> int:
    try:
        config = load_config()
    except Exception as e:
        logging.basicConfig(level=logging.ERROR)
        logging.error("gateway_config | result=error | error=%s", e)
        return 2

    initialize_log(config.logging_level)
    logging.info(
        "gateway_config | result=ok | host=%s | port=%s | mom=%s | "
        "file_ingestor_exchange=%s | file_ingestor_partitions=%s",
        config.server_host,
        config.server_port,
        config.mom_host,
        config.file_ingestor_exchange,
        config.file_ingestor_partitions,
    )

    gw = Gateway(config)
    signal.signal(signal.SIGTERM, lambda *_: gw.stop())

    gw.run()
    return 0


def load_config() -> GatewayConfig:
    port = get_int("SERVER_PORT", DEFAULT_SERVER_PORT)
    if port < MIN_TCP_PORT or port > MAX_TCP_PORT:
        raise ValueError(f"SERVER_PORT must be in range [{MIN_TCP_PORT}, {MAX_TCP_PORT}]")

    file_ingestor_partitions = get_int(
        "FILE_INGESTOR_PARTITIONS",
        DEFAULT_FILE_INGESTOR_PARTITIONS,
    )
    if file_ingestor_partitions <= 0:
        raise ValueError("FILE_INGESTOR_PARTITIONS must be greater than 0")

    return GatewayConfig(
        server_host=os.getenv("SERVER_HOST", DEFAULT_SERVER_HOST),
        server_port=port,
        mom_host=os.getenv("MOM_HOST", DEFAULT_MOM_HOST),
        file_ingestor_exchange=os.getenv(
            "FILE_INGESTOR_EXCHANGE",
            DEFAULT_FILE_INGESTOR_EXCHANGE,
        ),
        file_ingestor_partitions=file_ingestor_partitions,
        file_splitter_queue_prefix=os.getenv(
            "FILE_SPLITTER_QUEUE_PREFIX",
            DEFAULT_FILE_SPLITTER_QUEUE_PREFIX,
        ),
        logging_level=os.getenv("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL),
    )


def initialize_log(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


if __name__ == "__main__":
    raise SystemExit(main())
