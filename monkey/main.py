import os
import time
import random
import signal
import logging
from manager import MonkeyManager, excluded_from_env, included_from_env

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)


def main():
    logging.info("Starting Monkey...")

    enabled = os.getenv("MONKEY_ENABLED", "false").lower() == "true"
    interval = int(os.getenv("MONKEY_INTERVAL", "30"))
    interval_min = int(os.getenv("MONKEY_INTERVAL_MIN", str(interval)))
    interval_max = int(os.getenv("MONKEY_INTERVAL_MAX", str(interval)))
    interval_min, interval_max = sorted((interval_min, interval_max))
    # 0 means unlimited kills; otherwise stop after this many successful kills.
    max_kills = int(os.getenv("MONKEY_MAX_KILLS", "0"))
    # When true, the very first kill targets the current monitor leader (highest
    # id) to force a failover election; later kills hit kill-list workers.
    leader_first = os.getenv("MONKEY_KILL_MONITOR_LEADER_FIRST", "false").lower() == "true"
    # When true, the very first kill disconnects a random client mid-query to
    # exercise gateway ABORT + downstream per-client state flush. Takes
    # precedence over leader_first for the first kill. It fires after a short
    # fixed delay (not the worker interval) so it lands while the client is
    # still uploading, which is what makes the gateway broadcast the abort.
    client_first = os.getenv("MONKEY_KILL_CLIENT_FIRST", "false").lower() == "true"
    client_kill_delay = int(os.getenv("MONKEY_CLIENT_KILL_DELAY", "25"))

    if not enabled:
        logging.info("Monkey is disabled by configuration. Idling...")
        while True:
            time.sleep(3600)

    manager = MonkeyManager(
        excluded_containers=excluded_from_env(),
        included_containers=included_from_env(),
    )

    def manual_trigger(signum, frame):
        logging.info("Manual trigger received (SIGUSR1)")
        manager.kill_random_container()

    signal.signal(signal.SIGUSR1, manual_trigger)

    cap = str(max_kills) if max_kills else "unlimited"
    logging.info(
        "monkey_plan | interval=%s-%ss | max_kills=%s | first_kill_client=%s | "
        "first_kill_monitor_leader=%s | kill_list=%s | message=monkey plan ready",
        interval_min,
        interval_max,
        cap,
        client_first,
        leader_first,
        ",".join(included_from_env()) or "<any non-excluded>",
    )

    kills_done = 0
    while True:
        seq = kills_done + 1
        first_kill = kills_done == 0
        if client_first and first_kill:
            category = "client"
            delay = client_kill_delay
        elif leader_first and first_kill:
            category = "monitor_leader"
            delay = random.randint(interval_min, interval_max)
        else:
            category = "worker"
            delay = random.randint(interval_min, interval_max)
        logging.info(
            "monkey_waiting | next=%s/%s | category=%s | sleeping=%ss | "
            "message=waiting before next kill",
            seq,
            cap,
            category,
            delay,
        )
        time.sleep(delay)

        if category == "client":
            result = manager.kill_client()
        elif category == "monitor_leader":
            result = manager.kill_monitor_leader()
        else:
            result = manager.kill_random_container()

        if result:
            kills_done += 1
            aftermath = (
                "client disconnected; expecting gateway ABORT + downstream flush"
                if category == "client"
                else "awaiting monitor recovery"
            )
            logging.info(
                "monkey_killed | seq=%s/%s | category=%s | target=%s | "
                "message=kill confirmed; %s",
                kills_done,
                cap,
                category,
                result,
                aftermath,
            )
        else:
            logging.warning(
                "monkey_kill_missed | seq=%s | category=%s | "
                "message=nothing killed this cycle; will retry next interval",
                seq,
                category,
            )

        if max_kills and kills_done >= max_kills:
            logging.info(
                "monkey_done | kills=%s | message=reached max kills; idling",
                kills_done,
            )
            break

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
