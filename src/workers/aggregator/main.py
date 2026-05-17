import logging
import signal

from aggregators import AggregatorWorker


def main():
    logging.basicConfig(level=logging.INFO)
    aggregator_worker = AggregatorWorker()
    signal.signal(signal.SIGTERM, lambda signum, frame: aggregator_worker.handle_sigterm())
    aggregator_worker.start()
    return 0


if __name__ == "__main__":
    main()