import logging
import signal

from common.heartbeat import HeartbeatSender
from deduper import Q4DeduperWorker


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    worker = Q4DeduperWorker()
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
