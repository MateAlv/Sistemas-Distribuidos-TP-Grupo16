import logging
import signal

from workers.filter.filters import FilterWorker

def main():
    logging.basicConfig(level=logging.INFO)
    filter_worker = FilterWorker()
    signal.signal(signal.SIGTERM, lambda signum, frame: filter_worker.handle_sigterm())
    filter_worker.start()
    return 0

if __name__ == "__main__":
    main()
