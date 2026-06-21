#!/usr/bin/env python3
"""Crash-recovery smoke test for the Q5 aggregator.

Scenarios
---------
  smoke   1 aggregator (AGGREGATION_AMOUNT=1, N=1 path). Kill during DATA.
          Tests WAL replay + N=1 EOF path. Does NOT exercise FLUSH_ACK fix.

  A       2 aggregators. Kill the NON-LEADER (agg_1) during DATA.
          On recovery, agg_1 receives FLUSH_ORDER from agg_0.
          Tests: _CTRL_NS_FLUSH_ORDER namespace fix in _handle_control.

  B       2 aggregators. Kill the LEADER (agg_0) during DATA.
          On recovery, agg_0 receives redelivered FLUSH_ACKs from agg_1.
          Tests: _CTRL_NS_FLUSH_ACK namespace fix in _handle_response.

Usage
-----
  python scripts/crash_test_aggregator.py --scenario smoke
  python scripts/crash_test_aggregator.py --scenario A
  python scripts/crash_test_aggregator.py --scenario B
  python scripts/crash_test_aggregator.py --scenario A --dataset LI-Small --keep
"""

import argparse
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import reference_results as ref  # noqa: E402  (after path insert)

PROJECT = "crash-test-q5"
COMPOSE_FILE = ROOT / "docker-compose.crash-test.yaml"

# Pattern logged exactly once when the aggregator finishes startup and starts
# consuming.  Fires regardless of message volume, so it works with small datasets.
# Format: "aggregation_start | configuration=Q5 | id=<ID> | ..."
_START_LOG_PREFIX = "aggregation_start | configuration=Q5 | id="

# Seconds to sleep after the start log before killing, so the worker
# has time to process at least a few messages and write WAL entries.
KILL_DELAY_AFTER_START = 10

# Seconds to wait for the kill-trigger log before giving up.
TRIGGER_TIMEOUT = 120

# Seconds to wait for clients after restart.
CLIENT_TIMEOUT = 300


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def compose(*args):
    return ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), *args]


def run(cmd, check=False, **kw):
    return subprocess.run(cmd, check=check, **kw)


def quiet(cmd):
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def die(msg, code=1):
    print(f"\n✗ {msg}", file=sys.stderr)
    sys.exit(code)


# ---------------------------------------------------------------------------
# log watcher: blocks until a pattern appears in container logs
# ---------------------------------------------------------------------------

class LogWatcher(threading.Thread):
    """Streams `docker logs -f <container>` and fires an event on pattern match."""

    def __init__(self, container: str, pattern: str):
        super().__init__(daemon=True)
        self.container = container
        self.pattern = pattern
        self.matched = threading.Event()
        self._stop = threading.Event()

    def run(self):
        proc = subprocess.Popen(
            ["docker", "logs", "-f", "--tail", "100", self.container],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            for line in proc.stdout:
                if self._stop.is_set():
                    break
                if self.pattern in line:
                    self.matched.set()
                    break
        finally:
            proc.terminate()

    def wait_for_match(self, timeout: float) -> bool:
        return self.matched.wait(timeout=timeout)

    def stop(self):
        self._stop.set()


# ---------------------------------------------------------------------------
# compose lifecycle
# ---------------------------------------------------------------------------

def generate_compose(scenario: str, dataset: str):
    if scenario == "smoke":
        cmd = [
            sys.executable, str(ROOT / "scripts" / "generate_compose.py"),
            "--preset", "q5-test",
            "--dataset", dataset,
            "--test-output", str(COMPOSE_FILE),
            "--skip-output",
        ]
    else:
        cmd = [
            sys.executable, str(ROOT / "scripts" / "generate_compose.py"),
            "--config", str(ROOT / "config" / "crash-test-q5-2agg.yaml"),
            "--dataset", dataset,
            "--test-output", str(COMPOSE_FILE),
            "--skip-output",
        ]
    log(f"Generating compose ({scenario}, dataset={dataset})...")
    run(cmd, check=True)


def teardown():
    log("Tearing down stack...")
    quiet(compose("down", "--volumes", "--remove-orphans"))
    # also nuke leftover containers by project label
    ids = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}"],
        capture_output=True, text=True,
    ).stdout.split()
    if ids:
        quiet(["docker", "rm", "-f", *ids])


def build_and_start():
    log("Building images and starting stack...")
    run(compose("up", "--build", "--remove-orphans", "--detach"), check=True)


def restart_container(name: str):
    log(f"Restarting {name}...")
    run(compose("up", "--detach", name), check=True)


def wait_for_client(timeout: int) -> bool:
    log(f"Waiting for client_0 (timeout={timeout}s)...")
    result = run(compose("wait", "client_0"), timeout=timeout,
                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0


def clear_output():
    out = ROOT / "data" / "output"
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("results_q*.csv"):
        f.unlink()


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def validate(dataset: str) -> bool:
    dataset_dir = ROOT / "data" / "datasets" / dataset
    trans = f"{dataset}_Trans.csv"

    log("Validating Q5 output...")
    env = {**os.environ, "Q5_DATASET_DIR": str(dataset_dir), "Q5_DATASET_TRANS": trans}
    result = run(
        [sys.executable, str(ROOT / "scripts" / "validate_q5_output.py")],
        env=env,
    )
    return result.returncode == 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    scenario = args.scenario
    dataset = args.dataset

    # Resolve target container and kill-trigger based on scenario.
    if scenario == "smoke":
        target = "aggregation_q5_0"   # only one aggregator
        kill_trigger_id = "0"
    elif scenario == "A":
        target = "aggregation_q5_1"   # non-leader
        kill_trigger_id = "1"
    elif scenario == "B":
        target = "aggregation_q5_0"   # leader
        kill_trigger_id = "0"
    else:
        die(f"unknown scenario: {scenario}")

    kill_pattern = f"{_START_LOG_PREFIX}{kill_trigger_id}"

    log(f"=== Crash test: scenario={scenario} target={target} dataset={dataset} ===")
    print()

    generate_compose(scenario, dataset)
    teardown()
    clear_output()
    build_and_start()

    # ---- wait for target to finish startup ----
    log(f"Waiting for '{kill_pattern}' in {target} logs...")
    watcher = LogWatcher(target, kill_pattern)
    watcher.start()

    if not watcher.wait_for_match(TRIGGER_TIMEOUT):
        teardown()
        die(
            f"Timed out after {TRIGGER_TIMEOUT}s waiting for start log in {target}.\n"
            f"  Check 'docker logs {target}' for errors."
        )
    watcher.stop()

    # ---- let it process some DATA so WAL has entries to recover ----
    log(f"Sleeping {KILL_DELAY_AFTER_START}s so {target} writes WAL entries...")
    time.sleep(KILL_DELAY_AFTER_START)

    # ---- kill ----
    log(f"Killing {target} with SIGKILL...")
    result = subprocess.run(["docker", "kill", target], capture_output=True, text=True)
    if result.returncode != 0:
        teardown()
        die(f"docker kill failed: {result.stderr.strip()}")

    time.sleep(1)  # let RabbitMQ detect the disconnect and requeue unACKed messages

    # ---- restart ----
    restart_container(target)

    # ---- wait for completion ----
    try:
        finished = wait_for_client(CLIENT_TIMEOUT)
    except subprocess.TimeoutExpired:
        finished = False

    if not finished:
        log("WARNING: client_0 did not finish within timeout — dumping tail logs")
        run(compose("logs", "--tail", "50", target))
        teardown()
        die(f"client_0 did not finish after crash+restart of {target}")

    # ---- validate ----
    ok = validate(dataset)

    if not args.keep:
        teardown()
    else:
        log(
            f"--keep set. Stack is up. Tear down with:\n"
            f"  docker compose -p {PROJECT} -f {COMPOSE_FILE} down --volumes --remove-orphans"
        )

    print()
    if ok:
        print(ref.green(f"✓✓✓ CRASH TEST PASSED (scenario={scenario}) ✓✓✓"))
        return 0
    else:
        print(ref.red(f"✗✗✗ CRASH TEST FAILED (scenario={scenario}) ✗✗✗"))
        return 1


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--scenario", choices=["smoke", "A", "B"], required=True,
        help="smoke=1 agg kill during data; A=kill non-leader; B=kill leader",
    )
    p.add_argument(
        "--dataset", default="LI-Small",
        choices=["LI-Mini", "LI-Small"],
        help="Dataset to use (default: LI-Small)",
    )
    p.add_argument(
        "--keep", action="store_true", default=False,
        help="Leave the stack up after the test (for log inspection)",
    )
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(main())
