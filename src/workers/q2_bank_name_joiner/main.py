import logging
import os
import signal

try:
    from bank_name_joiner import BankNameJoinerConfig, BankNameJoinerWorker
except ImportError:
    from workers.q2_bank_name_joiner.bank_name_joiner import (
        BankNameJoinerConfig,
        BankNameJoinerWorker,
    )

from common.heartbeat import HeartbeatSender


DEFAULT_ID = 0
DEFAULT_MOM_HOST = "rabbitmq"
DEFAULT_Q2_INPUT_QUEUE = "q2_enrich_queue"
DEFAULT_ACCOUNTS_INPUT_QUEUE = "accounts_line_batch_queue"
DEFAULT_OUTPUT_QUEUE = "join_q2_results_queue"
DEFAULT_LOGGING_LEVEL = "INFO"


def main() -> int:
    try:
        config = load_config()
    except Exception as e:
        logging.basicConfig(level=logging.ERROR)
        logging.error("q2_bank_name_joiner_config | result=error | error=%s", e)
        return 2

    initialize_log(os.getenv("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL))
    worker = BankNameJoinerWorker(config)
    heartbeat = HeartbeatSender()
    heartbeat.start()

    def shutdown(*_):
        heartbeat.stop()
        worker.stop()

    signal.signal(signal.SIGTERM, shutdown)
    try:
        worker.start()
    finally:
        worker.close()
    return 0


def load_config() -> BankNameJoinerConfig:
    worker_id = get_int("ID", DEFAULT_ID)
    if worker_id < 0:
        raise ValueError("ID must be greater than or equal to 0")

    return BankNameJoinerConfig(
        id=worker_id,
        mom_host=os.getenv("MOM_HOST", DEFAULT_MOM_HOST),
        q2_input_queue=os.getenv("Q2_INPUT_QUEUE", DEFAULT_Q2_INPUT_QUEUE),
        accounts_input_queue=os.getenv(
            "ACCOUNTS_INPUT_QUEUE", DEFAULT_ACCOUNTS_INPUT_QUEUE
        ),
        output_queue=os.getenv("OUTPUT_QUEUE", DEFAULT_OUTPUT_QUEUE),
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
