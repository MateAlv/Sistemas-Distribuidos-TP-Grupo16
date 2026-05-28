import logging
import signal

from account_deduper import Q4AccountDeduperWorker


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    worker = Q4AccountDeduperWorker()
    signal.signal(signal.SIGTERM, lambda *_: worker.handle_sigterm())
    try:
        worker.start()
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
