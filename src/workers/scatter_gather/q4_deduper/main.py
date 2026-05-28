import logging
import signal

from deduper import Q4DeduperWorker


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    worker = Q4DeduperWorker()
    signal.signal(signal.SIGTERM, lambda *_: worker.handle_sigterm())
    try:
        worker.start()
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
