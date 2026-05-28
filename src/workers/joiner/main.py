import logging
import signal

from joiners import JoinerWorker


def main():
    logging.basicConfig(level=logging.INFO)
    worker = JoinerWorker()
    signal.signal(signal.SIGTERM, lambda signum, frame: worker.handle_sigterm())
    worker.start()
    return 0


if __name__ == "__main__":
    main()
