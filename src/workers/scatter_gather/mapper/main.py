import logging
import signal

from mapper import ScatterGatherMapper


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    worker = ScatterGatherMapper()
    signal.signal(signal.SIGTERM, lambda *_: worker.handle_sigterm())
    try:
        worker.start()
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
