import logging
import signal

from detector import ScatterGatherDetector


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    worker = ScatterGatherDetector()
    signal.signal(signal.SIGTERM, lambda *_: worker.handle_sigterm())
    try:
        worker.start()
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
