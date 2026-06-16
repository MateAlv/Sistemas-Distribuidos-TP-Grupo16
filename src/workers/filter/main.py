import logging
import signal

from common.heartbeat import HeartbeatSender
from filters import FilterWorker

def main():
    logging.basicConfig(level=logging.INFO)
    filter_worker = FilterWorker()
    heartbeat = HeartbeatSender()
    heartbeat.start()

    def shutdown(*_):
        heartbeat.stop()
        filter_worker.handle_sigterm()

    signal.signal(signal.SIGTERM, shutdown)
    filter_worker.start()
    return 0

if __name__ == "__main__":
    main()
