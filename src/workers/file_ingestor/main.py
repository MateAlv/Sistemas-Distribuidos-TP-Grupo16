import logging
import os
import signal

from common.heartbeat import HeartbeatSender
from file_ingestor import FileIngestor, FileIngestorConfig


DEFAULT_ID = 0
DEFAULT_MOM_HOST = "rabbitmq"
DEFAULT_TRANSACTION_OUTPUT_EXCHANGE = "transaction_fanout_exchange"
DEFAULT_CONTROL_QUEUE_PREFIX = "file_ingestor_control"
DEFAULT_RESPONSE_QUEUE_PREFIX = "file_ingestor_response"
DEFAULT_LOGGING_LEVEL = "INFO"
DEFAULT_STATE_DIR = ""
DEFAULT_SNAPSHOT_INTERVAL = 1000


def main() -> int:
    try:
        config = load_config()
    except Exception as e:
        logging.basicConfig(level=logging.ERROR)
        logging.error("file_ingestor_config | result=error | error=%s", e)
        return 2

    initialize_log(config.logging_level)
    ingestor = FileIngestor(config)
    heartbeat = HeartbeatSender()
    heartbeat.start()

    def shutdown(*_):
        heartbeat.stop()
        ingestor.stop()

    signal.signal(signal.SIGTERM, shutdown)
    ingestor.start()
    return 0


def load_config() -> FileIngestorConfig:
    ingestor_id = get_int("ID", DEFAULT_ID)
    if ingestor_id < 0:
        raise ValueError("ID must be greater than or equal to 0")

    total_instances = get_int("FILE_INGESTOR_AMOUNT", None)
    if total_instances <= 0:
        raise ValueError("FILE_INGESTOR_AMOUNT must be greater than 0")

    return FileIngestorConfig(
        id=ingestor_id,
        total_instances=total_instances,
        mom_host=os.getenv("MOM_HOST", DEFAULT_MOM_HOST),
        queue_name=require_env("LINE_BATCH_INPUT_QUEUE"),
        input_exchange=require_env("LINE_BATCH_INPUT_EXCHANGE"),
        input_routing_prefix=require_env("LINE_BATCH_INPUT_ROUTING_PREFIX"),
        transaction_output_exchange=os.getenv(
            "TRANSACTION_OUTPUT_EXCHANGE",
            DEFAULT_TRANSACTION_OUTPUT_EXCHANGE,
        ),
        control_queue_prefix=os.getenv(
            "FILE_INGESTOR_CONTROL_QUEUE_PREFIX",
            DEFAULT_CONTROL_QUEUE_PREFIX,
        ),
        response_queue_prefix=os.getenv(
            "FILE_INGESTOR_RESPONSE_QUEUE_PREFIX",
            DEFAULT_RESPONSE_QUEUE_PREFIX,
        ),
        logging_level=os.getenv("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL),
        state_dir=os.getenv("STATE_DIR", DEFAULT_STATE_DIR),
        snapshot_interval=get_int("SNAPSHOT_INTERVAL", DEFAULT_SNAPSHOT_INTERVAL),
    )


def initialize_log(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_int(name: str, default: int | None) -> int:
    value = os.getenv(name)
    if value is None:
        if default is None:
            raise ValueError(f"{name} is required")
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
