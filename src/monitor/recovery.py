import logging
import os
import subprocess
from collections.abc import Mapping


DEFAULT_COMPOSE_PROJECT_NAME = "distribuidos"
PINNED_CONTAINER_NAMES_ENV = "PINNED_CONTAINER_NAMES"


def container_name(
    node_id: str,
    env: Mapping[str, str] = os.environ,
) -> str:
    if env.get(PINNED_CONTAINER_NAMES_ENV, "").lower() == "true":
        return node_id

    project_name = env.get(
        "COMPOSE_PROJECT_NAME",
        DEFAULT_COMPOSE_PROJECT_NAME,
    ).strip()
    if not project_name:
        project_name = DEFAULT_COMPOSE_PROJECT_NAME

    return f"{project_name}-{node_id}-1"


def docker_start(
    node_id: str,
    env: Mapping[str, str] = os.environ,
) -> None:
    container = container_name(node_id, env)
    logging.info(
        "monitor_recovery_start | node_id=%s | container=%s",
        node_id,
        container,
    )

    try:
        result = subprocess.run(
            ["docker", "start", container],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        logging.error(
            "monitor_recovery_error | node_id=%s | container=%s | error=%s",
            node_id,
            container,
            exc,
        )
        return

    if result.returncode != 0:
        logging.warning(
            "monitor_recovery_failed | node_id=%s | container=%s | "
            "returncode=%s | stderr=%s",
            node_id,
            container,
            result.returncode,
            result.stderr.strip(),
        )
        return

    logging.info(
        "monitor_recovery_success | node_id=%s | container=%s",
        node_id,
        container,
    )
