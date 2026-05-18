import logging
import signal

from sums import SumWorker

def main():
    logging.basicConfig(level=logging.INFO)
    filter_worker = SumWorker()
    signal.signal(signal.SIGTERM, lambda signum, frame: filter_worker.handle_sigterm())
    filter_worker.start()
    return 0

if __name__ == "__main__":
    main()
