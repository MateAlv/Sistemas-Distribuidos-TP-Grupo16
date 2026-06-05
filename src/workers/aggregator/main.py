import logging
import signal

from common.heartbeat import HeartbeatSender
from aggregators import AggregatorWorker


def main():
    logging.basicConfig(level=logging.INFO)
    aggregator_worker = AggregatorWorker()
    heartbeat = HeartbeatSender()
    heartbeat.start()

    def shutdown(*_):
        heartbeat.stop()
        aggregator_worker.handle_sigterm()

    signal.signal(signal.SIGTERM, shutdown)
    aggregator_worker.start()
    return 0


if __name__ == "__main__":
    main()