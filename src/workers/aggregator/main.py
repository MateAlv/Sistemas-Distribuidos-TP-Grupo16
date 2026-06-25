import logging
import os
import signal

from common.heartbeat import HeartbeatSender
from aggregators import AggregatorWorker


DEFAULT_LOGGING_LEVEL = "INFO"


def main():
    initialize_log(os.getenv("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL))
    worker = AggregatorWorker()
    heartbeat = HeartbeatSender()
    heartbeat.start()

    def shutdown(*_):
        heartbeat.stop()
        worker.handle_sigterm()

    signal.signal(signal.SIGTERM, shutdown)
    worker.start()
    return 0


def initialize_log(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


if __name__ == "__main__":
    raise SystemExit(main())