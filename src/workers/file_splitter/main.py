import logging
import os
import signal

from common.heartbeat import HeartbeatSender
from file_splitter import FileSplitter, FileSplitterConfig


DEFAULT_ID = 0
DEFAULT_MOM_HOST = "rabbitmq"
DEFAULT_INPUT_EXCHANGE = "file_ingestor_exchange"
DEFAULT_QUEUE_PREFIX = "file_splitter"
DEFAULT_MAX_LINE_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_BATCH_BYTES = 64 * 1024
DEFAULT_LOGGING_LEVEL = "INFO"


def main() -> int:
    try:
        config = load_config()
    except Exception as e:
        logging.basicConfig(level=logging.ERROR)
        logging.error("file_splitter_config | result=error | error=%s", e)
        return 2

    initialize_log(config.logging_level)
    splitter = FileSplitter(config)
    heartbeat = HeartbeatSender()
    heartbeat.start()

    def shutdown(*_):
        heartbeat.stop()
        splitter.stop()

    signal.signal(signal.SIGTERM, shutdown)
    splitter.start()
    return 0


def load_config() -> FileSplitterConfig:
    splitter_id = get_int("ID", DEFAULT_ID)
    if splitter_id < 0:
        raise ValueError("ID must be greater than or equal to 0")

    max_line_bytes = get_int("MAX_LINE_BYTES", DEFAULT_MAX_LINE_BYTES)
    if max_line_bytes <= 0:
        raise ValueError("MAX_LINE_BYTES must be greater than 0")

    max_batch_bytes = get_int("MAX_BATCH_BYTES", DEFAULT_MAX_BATCH_BYTES)
    if max_batch_bytes <= 0:
        raise ValueError("MAX_BATCH_BYTES must be greater than 0")

    output_shard_count = get_int("FILE_INGESTOR_AMOUNT", None)
    if output_shard_count <= 0:
        raise ValueError("FILE_INGESTOR_AMOUNT must be greater than 0")

    return FileSplitterConfig(
        id=splitter_id,
        mom_host=os.getenv("MOM_HOST", DEFAULT_MOM_HOST),
        input_exchange=os.getenv("FILE_SPLITTER_INPUT_EXCHANGE", DEFAULT_INPUT_EXCHANGE),
        queue_name=file_splitter_queue_name(splitter_id),
        output_exchange=require_env("LINE_BATCH_OUTPUT_EXCHANGE"),
        output_routing_prefix=require_env("LINE_BATCH_OUTPUT_ROUTING_PREFIX"),
        output_shard_count=output_shard_count,
        max_line_bytes=max_line_bytes,
        max_batch_bytes=max_batch_bytes,
        logging_level=os.getenv("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL),
        input_routing_key=os.getenv("FILE_SPLITTER_INPUT_ROUTING_KEY"),
        accounts_output_queue=os.getenv("ACCOUNTS_LINE_BATCH_OUTPUT_QUEUE"),
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


def file_splitter_queue_name(splitter_id: int) -> str:
    prefix = os.getenv("FILE_SPLITTER_QUEUE_PREFIX", DEFAULT_QUEUE_PREFIX)
    return f"{prefix}_{splitter_id}"


if __name__ == "__main__":
    raise SystemExit(main())
