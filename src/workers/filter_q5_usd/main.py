import logging
import signal

from common.heartbeat import HeartbeatSender
from filter_q5_usd import FilterQ5UsdWorker


def main():
    logging.basicConfig(level=logging.INFO)
    worker = FilterQ5UsdWorker()
    heartbeat = HeartbeatSender()
    heartbeat.start()

    def shutdown(*_):
        heartbeat.stop()
        worker.handle_sigterm()

    signal.signal(signal.SIGTERM, shutdown)
    worker.start()
    return 0


if __name__ == "__main__":
    main()
