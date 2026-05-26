import logging
import signal

from q3_barrier import Q3BarrierWorker


def main():
    logging.basicConfig(level=logging.INFO)
    worker = Q3BarrierWorker()
    signal.signal(signal.SIGTERM, lambda signum, frame: worker.handle_sigterm())
    worker.start()
    return 0


if __name__ == "__main__":
    main()
