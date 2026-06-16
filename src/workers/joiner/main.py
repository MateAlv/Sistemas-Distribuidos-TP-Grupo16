import logging
import signal

from common.heartbeat import HeartbeatSender
from joiners import JoinerWorker


def main():
    logging.basicConfig(level=logging.INFO)
    worker = JoinerWorker()
    heartbeat = HeartbeatSender()
    heartbeat.start()

    def shutdown(*_):
        heartbeat.stop()
        worker.handle_sigterm()

    signal.signal(signal.SIGTERM, shutdown)
    worker.start()
    return 0


if __name__ == "__main__":
    main()
