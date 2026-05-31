import subprocess
import random
import logging

class ChaosManager:
    def __init__(self, excluded_containers=None):
        self.excluded_containers = excluded_containers or []
        # Always exclude self and infrastructure
        self.excluded_containers.extend(["rabbitmq", "chaos_monkey"])

    def get_running_containers(self):
        """Returns a list of running container names."""
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

    def get_valid_targets(self):
        """Returns a list of containers that are valid targets for chaos."""
        containers = self.get_running_containers()
        targets = []
        for container in containers:
            is_excluded = False
            for excluded in self.excluded_containers:
                if excluded in container:
                    is_excluded = True
                    break
            
            if "client" in container:
                is_excluded = True

            if not is_excluded:
                targets.append(container)
        return targets

    def kill_random_container(self):
        """Kills a random valid target container."""
        targets = self.get_valid_targets()
        if not targets:
            logging.warning("No valid targets found to kill.")
            return None

        target = random.choice(targets)
        return self.kill_container(target)

    def kill_container(self, container_name):
        """Kills a specific container."""
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
