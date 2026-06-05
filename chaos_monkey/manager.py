import os
import subprocess
import random
import logging

# Substrings protected from kills: broker, entry point, rates edge, clients, self.
DEFAULT_EXCLUDED = ["rabbitmq", "gateway", "rates_service", "client", "chaos"]


def excluded_from_env(env=None):
    env = os.environ if env is None else env
    extra = [token.strip() for token in env.get("CHAOS_EXCLUDE", "").split(",") if token.strip()]
    return list(DEFAULT_EXCLUDED) + extra


class ChaosManager:
    def __init__(self, excluded_containers=None):
        if excluded_containers is None:
            excluded_containers = list(DEFAULT_EXCLUDED)
        self.excluded_containers = excluded_containers

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

    def get_valid_targets(self):
        return [c for c in self.get_running_containers() if not self.is_excluded(c)]

    def kill_random_container(self):
        targets = self.get_valid_targets()
        if not targets:
            logging.warning("No valid targets found to kill.")
            return None

        target = random.choice(targets)
        return self.kill_container(target)

    def kill_container(self, container_name):
        logging.info(f"action: kill_container | target: {container_name} | status: starting")
        try:
            subprocess.run(
                ["docker", "kill", container_name],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            logging.info(f"action: kill_container | target: {container_name} | status: success")
            return container_name
        except subprocess.CalledProcessError as e:
            logging.error(f"action: kill_container | target: {container_name} | status: failed | error: {e}")
            return None
