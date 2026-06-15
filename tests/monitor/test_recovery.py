import logging
import subprocess

import pytest

import monitor.recovery as recovery_module
from monitor.recovery import (
    DEFAULT_COMPOSE_PROJECT_NAME,
    container_name,
    docker_start,
)


def test_container_name_uses_compose_project_name() -> None:
    assert (
        container_name(
            "filter_usd_0",
            {"COMPOSE_PROJECT_NAME": "grupo16"},
        )
        == "grupo16-filter_usd_0-1"
    )


@pytest.mark.parametrize(
    "env",
    [{}, {"COMPOSE_PROJECT_NAME": ""}, {"COMPOSE_PROJECT_NAME": "  "}],
)
def test_container_name_uses_default_project(env: dict[str, str]) -> None:
    assert (
        container_name("q4_sum_1", env)
        == f"{DEFAULT_COMPOSE_PROJECT_NAME}-q4_sum_1-1"
    )


def test_container_name_supports_pinned_compose_names() -> None:
    assert (
        container_name(
            "filter_usd_0",
            {"PINNED_CONTAINER_NAMES": "true"},
        )
        == "filter_usd_0"
    )


def test_docker_start_invokes_docker_without_raising_on_failure(monkeypatch) -> None:
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, returncode=1, stdout="", stderr="container is already running")

    monkeypatch.setattr(recovery_module.subprocess, "run", fake_run)

    docker_start("filter_usd_0", env={"COMPOSE_PROJECT_NAME": "grupo16"})

    assert calls == [
        (
            ["docker", "start", "grupo16-filter_usd_0-1"],
            {"check": False, "capture_output": True, "text": True},
        )
    ]


def test_docker_start_logs_os_error(monkeypatch, caplog) -> None:
    def fake_run(*_args, **_kwargs):
        raise FileNotFoundError("docker not found")

    monkeypatch.setattr(recovery_module.subprocess, "run", fake_run)

    with caplog.at_level(logging.ERROR):
        docker_start("worker_0", env={})

    assert "monitor_recovery_error" in caplog.text
    assert "docker not found" in caplog.text
