#!/usr/bin/env python3
"""Crash-recovery smoke test for the q3_barrier worker on Q3.

The worker joins two input streams per client: Q3 averages from join_q3 and
candidate transactions from the date/usd filter path. These scenarios target
the recovery windows that matter for that barrier.

Scenarios
---------
  smoke   Kill after startup and a short delay, while DATA is being processed.
          Exercises WAL replay for averages and candidate disk-log state.

  A       Kill right after an averages EOF is observed.
          Exercises recovery when the averages side of the barrier is complete.

  B       Kill right after a candidates EOF is observed.
          Exercises recovery when the candidate side may be ready to emit.

Usage
-----
  venv/bin/python scripts/crash_test_q3_barrier.py --scenario smoke --dataset LI-Small
  venv/bin/python scripts/crash_test_q3_barrier.py --scenario A
  venv/bin/python scripts/crash_test_q3_barrier.py --scenario B --keep
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from crash_test_paths import resolve_compose_file  # noqa: E402
import reference_results as ref  # noqa: E402


COMPOSE_FILE = ROOT / "tmp" / "crash-tests" / "docker-compose.q3-barrier.yaml"
PROJECT = "crash-test-q3-barrier"
DEFAULT_DATASET = "LI-Small"
TARGET = "q3_barrier_0"

TRIGGER_TIMEOUT = 120
CLIENT_TIMEOUT = 300

_SCENARIOS = {
    "smoke": {
        "pattern": "q3_barrier_start | id=0",
        "delay": 10,
        "description": "kill during DATA after startup",
    },
    "A": {
        "pattern": "q3_barrier_avg_eof | id=0",
        "delay": 0,
        "description": "kill after averages EOF",
    },
    "B": {
        "pattern": "q3_barrier_candidate_eof | id=0",
        "delay": 0,
        "description": "kill after candidates EOF / ready edge",
    },
}


def compose(*args):
    return [
        "docker",
        "compose",
        "--project-directory",
        str(ROOT),
        "-p",
        PROJECT,
        "-f",
        str(COMPOSE_FILE),
        *args,
    ]


def run(cmd, check=False, **kw):
    return subprocess.run(cmd, check=check, **kw)


def quiet(cmd):
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    print(f"\n{ref.red('ERROR')} {msg}", file=sys.stderr)
    sys.exit(code)


class LogWatcher(threading.Thread):
    def __init__(self, container: str, pattern: str) -> None:
        super().__init__(daemon=True)
        self.container = container
        self.pattern = pattern
        self.matched = threading.Event()
        self._stop_requested = threading.Event()

    def run(self) -> None:
        proc = subprocess.Popen(
            ["docker", "logs", "-f", "--tail", "100", self.container],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                if self._stop_requested.is_set():
                    break
                if self.pattern in line:
                    self.matched.set()
                    break
        finally:
            proc.terminate()

    def wait_for_match(self, timeout: float) -> bool:
        return self.matched.wait(timeout=timeout)

    def stop(self) -> None:
        self._stop_requested.set()


def generate_compose(dataset: str) -> None:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "generate_compose.py"),
        "--preset",
        "q3-test",
        "--dataset",
        dataset,
        "--test-output",
        str(COMPOSE_FILE),
        "--skip-output",
    ]
    log(f"Generating compose (q3-test, dataset={dataset})...")
    run(cmd, check=True)


def teardown(keep: bool) -> None:
    if keep:
        log("--keep set: leaving stack running.")
        return
    log("Tearing down stack...")
    quiet(compose("down", "--volumes", "--remove-orphans"))
    ids = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}"],
        capture_output=True,
        text=True,
    ).stdout.split()
    if ids:
        quiet(["docker", "rm", "-f", *ids])


def build_and_start() -> None:
    log("Building images and starting stack...")
    run(compose("up", "--build", "--remove-orphans", "--detach"), check=True)


def restart_container() -> None:
    log(f"Restarting {TARGET}...")
    run(compose("up", "--detach", TARGET), check=True)


def wait_for_client(timeout: int) -> bool:
    log(f"Waiting for client_0 (timeout={timeout}s)...")
    result = run(
        compose("wait", "client_0"),
        timeout=timeout,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def clear_output() -> None:
    out = ROOT / "data" / "output"
    out.mkdir(parents=True, exist_ok=True)
    for path in out.glob("results_q3*.csv"):
        path.unlink()


def validate(dataset: str) -> bool:
    dataset_dir = ROOT / "data" / "datasets" / dataset
    log("Validating Q3 output...")
    env = {
        **os.environ,
        "Q3_DATASET_DIR": str(dataset_dir),
        "Q3_DATASET_TRANS": f"{dataset}_Trans.csv",
    }
    result = run(
        [sys.executable, str(ROOT / "scripts" / "validate_q3_output.py")],
        env=env,
    )
    return result.returncode == 0


def main() -> int:
    global COMPOSE_FILE

    args = parse_args()
    scenario = args.scenario
    dataset = args.dataset
    COMPOSE_FILE = resolve_compose_file(
        ROOT,
        args.compose_file,
        "q3-barrier",
        scenario,
        dataset,
    )
    cfg = _SCENARIOS[scenario]

    log(
        "=== Crash test: worker=q3_barrier "
        f"scenario={scenario} target={TARGET} dataset={dataset} ==="
    )
    log(f"Scenario detail: {cfg['description']}")
    print()

    generate_compose(dataset)
    teardown(keep=False)
    clear_output()
    build_and_start()

    pattern = cfg["pattern"]
    log(f"Waiting for '{pattern}' in {TARGET} logs...")
    watcher = LogWatcher(TARGET, pattern)
    watcher.start()

    if not watcher.wait_for_match(TRIGGER_TIMEOUT):
        teardown(args.keep)
        die(
            f"Timed out after {TRIGGER_TIMEOUT}s waiting for trigger in {TARGET}.\n"
            f"  Check 'docker logs {TARGET}' for errors."
        )
    watcher.stop()

    delay = cfg["delay"]
    if delay > 0:
        log(f"Sleeping {delay}s so {TARGET} writes WAL entries...")
        time.sleep(delay)

    log(f"Killing {TARGET} with SIGKILL...")
    result = subprocess.run(["docker", "kill", TARGET], capture_output=True, text=True)
    if result.returncode != 0:
        teardown(args.keep)
        die(f"docker kill failed: {result.stderr.strip()}")

    time.sleep(1)
    restart_container()

    try:
        finished = wait_for_client(CLIENT_TIMEOUT)
    except subprocess.TimeoutExpired:
        finished = False

    if not finished:
        log("WARNING: client_0 did not finish within timeout - dumping tail logs")
        run(compose("logs", "--tail", "50", TARGET))
        teardown(args.keep)
        die(f"client_0 did not finish after crash+restart of {TARGET}")

    ok = validate(dataset)
    teardown(args.keep)

    print()
    if ok:
        print(ref.green(f"CRASH TEST PASSED (scenario={scenario})"))
        return 0
    print(ref.red(f"CRASH TEST FAILED (scenario={scenario})"))
    return 1


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        choices=["smoke", "A", "B"],
        default="smoke",
        help="Crash scenario to run (default: smoke)",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        default=False,
        help="Leave the stack up after the test (for log inspection)",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        choices=["LI-Mini", "LI-Small"],
        help=f"Dataset to use (default: {DEFAULT_DATASET})",
    )
    parser.add_argument(
        "--compose-file",
        help="Path for the generated compose file (default: tmp/crash-tests/...).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
