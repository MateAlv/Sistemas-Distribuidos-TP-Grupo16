import logging
import signal

from sums import Q4SumWorker


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    worker = Q4SumWorker()
    signal.signal(signal.SIGTERM, lambda *_: worker.handle_sigterm())
    try:
        worker.start()
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
