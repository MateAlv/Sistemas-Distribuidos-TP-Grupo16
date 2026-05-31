import os
import time
import signal
import logging
from manager import ChaosManager

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

def main():
    logging.info("Starting Chaos Monkey...")
    
    enabled = os.getenv("CHAOS_ENABLED", "false").lower() == "true"
    interval = int(os.getenv("CHAOS_INTERVAL", "30"))
    
    if not enabled:
        logging.info("Chaos Monkey is disabled by configuration. Idling...")
        while True:
            time.sleep(3600)

    manager = ChaosManager()

    def manual_trigger(signum, frame):
        logging.info("Manual trigger received (SIGUSR1)")
        manager.kill_random_container()

    signal.signal(signal.SIGUSR1, manual_trigger)
    
    logging.info(f"Chaos Monkey started. Interval: {interval}s")

    while True:
        time.sleep(interval)
        manager.kill_random_container()

if __name__ == "__main__":
    main()
