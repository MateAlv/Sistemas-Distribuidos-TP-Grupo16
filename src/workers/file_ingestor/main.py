import logging
import os
import signal

from file_ingestor import FileIngestor, FileIngestorConfig


DEFAULT_ID = 0
DEFAULT_MOM_HOST = "rabbitmq"
DEFAULT_FILE_INGESTOR_EXCHANGE = "file_ingestor_exchange"
DEFAULT_FILE_INGESTOR_QUEUE_PREFIX = "file_ingestor"
DEFAULT_TRANSACTION_OUTPUT_EXCHANGE = "transaction_fanout_exchange"
DEFAULT_MAX_LINE_BYTES = 16 * 1024 * 1024
DEFAULT_LOGGING_LEVEL = "INFO"


def main() -> int:
    try:
        config = load_config()
    except Exception as e:
        logging.basicConfig(level=logging.ERROR)
        logging.error("file_ingestor_config | result=error | error=%s", e)
        return 2

    initialize_log(config.logging_level)
    ingestor = FileIngestor(config)
    signal.signal(signal.SIGTERM, lambda *_: ingestor.stop())
    ingestor.start()
    return 0


def load_config() -> FileIngestorConfig:
    ingestor_id = get_int("ID", DEFAULT_ID)
    if ingestor_id < 0:
        raise ValueError("ID must be greater than or equal to 0")

    max_line_bytes = get_int("MAX_LINE_BYTES", DEFAULT_MAX_LINE_BYTES)
    if max_line_bytes <= 0:
        raise ValueError("MAX_LINE_BYTES must be greater than 0")

    return FileIngestorConfig(
        id=ingestor_id,
        mom_host=os.getenv("MOM_HOST", DEFAULT_MOM_HOST),
        file_ingestor_exchange=os.getenv(
            "FILE_INGESTOR_EXCHANGE",
            DEFAULT_FILE_INGESTOR_EXCHANGE,
        ),
        queue_name=file_ingestor_queue_name(ingestor_id),
        transaction_output_exchange=os.getenv(
            "TRANSACTION_OUTPUT_EXCHANGE",
            DEFAULT_TRANSACTION_OUTPUT_EXCHANGE,
        ),
        max_line_bytes=max_line_bytes,
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


def file_ingestor_queue_name(ingestor_id: int) -> str:
    prefix = os.getenv(
        "FILE_INGESTOR_QUEUE_PREFIX",
        DEFAULT_FILE_INGESTOR_QUEUE_PREFIX,
    )
    return f"{prefix}_{ingestor_id}"


if __name__ == "__main__":
    raise SystemExit(main())
