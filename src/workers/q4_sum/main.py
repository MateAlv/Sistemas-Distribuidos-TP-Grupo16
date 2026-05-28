import logging
import signal

from edge_store import Q4EdgeStoreWorker


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    worker = Q4EdgeStoreWorker()
    signal.signal(signal.SIGTERM, lambda *_: worker.handle_sigterm())
    try:
        worker.start()
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
