import logging
import signal

from common.heartbeat import HeartbeatSender
from q3_barrier import Q3BarrierWorker


def main():
    logging.basicConfig(level=logging.INFO)
    worker = Q3BarrierWorker()
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
