import logging
import signal

from filter_q5_usd import FilterQ5UsdWorker


def main():
    logging.basicConfig(level=logging.INFO)
    worker = FilterQ5UsdWorker()
    signal.signal(signal.SIGTERM, lambda signum, frame: worker.handle_sigterm())
    worker.start()
    return 0


if __name__ == "__main__":
    main()
