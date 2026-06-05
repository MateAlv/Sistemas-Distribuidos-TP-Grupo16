import logging
import signal

from common.heartbeat import HeartbeatSender
from aggregator import Q4AggregatorWorker


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    worker = Q4AggregatorWorker()
    heartbeat = HeartbeatSender()
    heartbeat.start()

    def shutdown(*_):
        heartbeat.stop()
        worker.handle_sigterm()

    signal.signal(signal.SIGTERM, shutdown)
    try:
        worker.start()
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
