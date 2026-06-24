import os
import time
import random
import signal
import logging
from manager import ChaosManager, excluded_from_env, included_from_env

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
    interval_min = int(os.getenv("CHAOS_INTERVAL_MIN", str(interval)))
    interval_max = int(os.getenv("CHAOS_INTERVAL_MAX", str(interval)))
    interval_min, interval_max = sorted((interval_min, interval_max))
    # 0 means unlimited kills; otherwise stop after this many successful kills.
    max_kills = int(os.getenv("CHAOS_MAX_KILLS", "0"))
    # When true, the very first kill targets the current monitor leader (highest
    # id) to force a failover election; later kills hit kill-list workers.
    leader_first = os.getenv("CHAOS_KILL_MONITOR_LEADER_FIRST", "false").lower() == "true"

    if not enabled:
        logging.info("Chaos Monkey is disabled by configuration. Idling...")
        while True:
            time.sleep(3600)

    manager = ChaosManager(
        excluded_containers=excluded_from_env(),
        included_containers=included_from_env(),
    )

    def manual_trigger(signum, frame):
        logging.info("Manual trigger received (SIGUSR1)")
        manager.kill_random_container()

    signal.signal(signal.SIGUSR1, manual_trigger)

    cap = str(max_kills) if max_kills else "unlimited"
    logging.info(
        "chaos_monkey_plan | interval=%s-%ss | max_kills=%s | "
        "first_kill_monitor_leader=%s | kill_list=%s | message=chaos plan ready",
        interval_min,
        interval_max,
        cap,
        leader_first,
        ",".join(included_from_env()) or "<any non-excluded>",
    )

    kills_done = 0
    while True:
        seq = kills_done + 1
        delay = random.randint(interval_min, interval_max)
        target_leader = leader_first and kills_done == 0
        logging.info(
            "chaos_monkey_waiting | next=%s/%s | category=%s | sleeping=%ss | "
            "message=waiting before next kill",
            seq,
            cap,
            "monitor_leader" if target_leader else "worker",
            delay,
        )
        time.sleep(delay)

        if target_leader:
            result = manager.kill_monitor_leader()
            category = "monitor_leader"
        else:
            result = manager.kill_random_container()
            category = "worker"

        if result:
            kills_done += 1
            logging.info(
                "chaos_monkey_killed | seq=%s/%s | category=%s | target=%s | "
                "message=kill confirmed; awaiting monitor recovery",
                kills_done,
                cap,
                category,
                result,
            )
        else:
            logging.warning(
                "chaos_monkey_kill_missed | seq=%s | category=%s | "
                "message=nothing killed this cycle; will retry next interval",
                seq,
                category,
            )

        if max_kills and kills_done >= max_kills:
            logging.info(
                "chaos_monkey_done | kills=%s | message=reached max kills; idling",
                kills_done,
            )
            break

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
