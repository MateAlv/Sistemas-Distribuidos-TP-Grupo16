import logging
import signal

from source_prefilter import Q4SourcePrefilterWorker


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    worker = Q4SourcePrefilterWorker()
    signal.signal(signal.SIGTERM, lambda *_: worker.handle_sigterm())
    try:
        worker.start()
    finally:
        worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
