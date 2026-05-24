import logging
import os
import signal

from file_ingestor import FileIngestor, FileIngestorConfig


DEFAULT_ID = 0
DEFAULT_MOM_HOST = "rabbitmq"
DEFAULT_LINE_BATCH_INPUT_QUEUE = "line_batch_queue"
DEFAULT_TRANSACTION_OUTPUT_QUEUE = "filter_usd_queue"
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

    return FileIngestorConfig(
        id=ingestor_id,
        mom_host=os.getenv("MOM_HOST", DEFAULT_MOM_HOST),
        queue_name=os.getenv("LINE_BATCH_INPUT_QUEUE", DEFAULT_LINE_BATCH_INPUT_QUEUE),
        transaction_output_queue=os.getenv(
            "TRANSACTION_OUTPUT_QUEUE",
            DEFAULT_TRANSACTION_OUTPUT_QUEUE,
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
