import os
import re
import subprocess
import random
import logging

# Substrings protected from kills: broker, entry point, rates edge, clients, self.
DEFAULT_EXCLUDED = ["rabbitmq", "gateway", "rates_service", "client", "chaos"]

_MONITOR_RE = re.compile(r"monitor_(\d+)")


def monitor_index(container):
    """Return the numeric id of a monitor container (monitor_3 -> 3), else None."""
    match = _MONITOR_RE.search(container)
    return int(match.group(1)) if match else None


def excluded_from_env(env=None):
    env = os.environ if env is None else env
    extra = [token.strip() for token in env.get("CHAOS_EXCLUDE", "").split(",") if token.strip()]
    return list(DEFAULT_EXCLUDED) + extra


def included_from_env(env=None):
    env = os.environ if env is None else env
    return [token.strip() for token in env.get("CHAOS_INCLUDE", "").split(",") if token.strip()]


class ChaosManager:
    def __init__(self, excluded_containers=None, included_containers=None):
        if excluded_containers is None:
            excluded_containers = list(DEFAULT_EXCLUDED)
        self.excluded_containers = excluded_containers
        # Kill list: when non-empty, only containers matching one of these
        # substrings are killable. Empty means "any non-excluded container".
        self.included_containers = list(included_containers or [])

    def get_running_containers(self):
        try:
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                check=True,
            )
            containers = result.stdout.strip().split('\n')
            return [c for c in containers if c]
        except subprocess.CalledProcessError as e:
            logging.error(
                "Failed to list containers: %s | stdout:%r | stderr:%r",
                e,
                getattr(e, "stdout", ""),
                getattr(e, "stderr", ""),
            )
            return []

    def is_excluded(self, container):
        return any(token in container for token in self.excluded_containers)

    def is_included(self, container):
        if not self.included_containers:
            return True
        return any(token in container for token in self.included_containers)

    def get_valid_targets(self):
        return [
            c
            for c in self.get_running_containers()
            if self.is_included(c) and not self.is_excluded(c)
        ]

    def monitor_leader(self):
        """Bully election: the highest-numbered running monitor is the leader."""
        indexed = [
            (monitor_index(c), c)
            for c in self.get_running_containers()
            if monitor_index(c) is not None
        ]
        return max(indexed)[1] if indexed else None

    def kill_random_container(self):
        kill_list = ",".join(self.included_containers) or "<any non-excluded>"
        targets = self.get_valid_targets()
        if not targets:
            logging.warning(
                "action: kill_random_container | status: no_targets | "
                "kill_list: %s | message: no running container matched the kill list",
                kill_list,
            )
            return None

        target = random.choice(targets)
        logging.info(
            "action: kill_random_container | target: %s | candidates: %d | "
            "kill_list: %s | message: randomly chose a worker from the kill list",
            target,
            len(targets),
            kill_list,
        )
        return self.kill_container(target, role="worker")

    def kill_monitor_leader(self):
        leader = self.monitor_leader()
        if leader is None:
            logging.warning(
                "action: kill_monitor_leader | status: no_monitor | "
                "message: no running monitor container found to target"
            )
            return None
        logging.info(
            "action: kill_monitor_leader | target: %s | reason: trigger_failover_election | "
            "message: killing current monitor leader (highest id) to force a re-election",
            leader,
        )
        return self.kill_container(leader, role="monitor_leader")

    def kill_container(self, container_name, role="worker"):
        logging.info(
            "action: kill_container | target: %s | role: %s | status: starting | "
            "message: sending SIGKILL",
            container_name,
            role,
        )
        try:
            subprocess.run(
                ["docker", "kill", container_name],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            logging.info(
                "action: kill_container | target: %s | role: %s | status: success | "
                "message: container killed (SIGKILL); monitor should detect and revive it",
                container_name,
                role,
            )
            return container_name
        except subprocess.CalledProcessError as e:
            logging.error(
                "action: kill_container | target: %s | role: %s | status: failed | error: %s",
                container_name,
                role,
                e,
            )
            return None
