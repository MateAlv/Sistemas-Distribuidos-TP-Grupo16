#!/usr/bin/env python3
"""Crash-recovery test for the filter_q5_usd worker.

Scenarios
---------
  smoke   1 filter_q5_usd (N=1 path). Kill during DATA processing.
          Tests WAL replay + N=1 upstream-EOF path (no coordination).

  A       2 filter_q5_usd instances. Kill the NON-LEADER (instance 1) during DATA.
          On recovery, instance 1 receives FLUSH_ORDER from the leader.
          Tests: CTRL_UPSTREAM_EOF namespace fix, FLUSH_ORDER non-leader path.

  B       2 filter_q5_usd instances. Kill the LEADER (instance 0) during DATA.
          On recovery, instance 0 receives redelivered FLUSH_ACKs from instance 1.
          Tests: CTRL_FLUSH_ACK dedup in _handle_response.

Usage
-----
  venv/bin/python scripts/crash_test_filter_q5_usd.py --scenario smoke
  venv/bin/python scripts/crash_test_filter_q5_usd.py --scenario A
  venv/bin/python scripts/crash_test_filter_q5_usd.py --scenario B --keep
"""

import argparse
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "docker-compose.test.yaml"
PROJECT = "crash-test-filter-q5-usd"
CONTAINER_PREFIX = "filter_q5_usd"

DATASET = "LI-Small"
TRIGGER_TIMEOUT = 120   # seconds to wait for startup log
KILL_DELAY_AFTER_START = 10  # seconds to let WAL fill before kill
CLIENT_TIMEOUT = 600    # seconds to wait for client_0 to finish


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def compose(project, *args):
    return ["docker", "compose", "-p", project, "-f", str(COMPOSE_FILE), *args]


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
# log watcher
# ---------------------------------------------------------------------------

class LogWatcher(threading.Thread):
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

def generate_compose(scenario: str):
    # chunk_max_bytes=64KB keeps each WAL record small.
    # filter_q5_usd emits one output packet per matching transaction, so a 1MB
    # batch can produce thousands of in-memory outputs simultaneously (outbox +
    # instruction.outputs + WAL buffer). Smaller chunks avoid OOMKill under test.
    if scenario == "smoke":
        config = "crash-test-filter-q5-usd-smoke.yaml"
    else:
        config = "crash-test-filter-q5-usd-2inst.yaml"

    cmd = [
        sys.executable, str(ROOT / "scripts" / "generate_compose.py"),
        "--config", str(ROOT / "config" / config),
        "--dataset", DATASET,
        "--test-output", str(COMPOSE_FILE),
        "--skip-output",
    ]
    log(f"Generating compose (scenario={scenario}, dataset={DATASET})...")
    run(cmd, check=True)


def teardown(keep: bool):
    if keep:
        log("--keep set: leaving stack running.")
        return
    log("Tearing down stack...")
    quiet(compose(PROJECT, "down", "--volumes", "--remove-orphans"))
    ids = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}"],
        capture_output=True, text=True,
    ).stdout.split()
    if ids:
        quiet(["docker", "rm", "-f", *ids])


def build_and_start():
    log("Building images and starting stack...")
    run(compose(PROJECT, "up", "--build", "--remove-orphans", "--detach"), check=True)


def restart_container(name: str):
    log(f"Restarting {name}...")
    run(compose(PROJECT, "up", "--detach", name), check=True)


def wait_for_client(timeout: int) -> bool:
    log(f"Waiting for client_0 (timeout={timeout}s)...")
    result = run(compose(PROJECT, "wait", "client_0"), timeout=timeout,
                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0


def clear_output():
    out = ROOT / "data" / "output"
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("results_q5*.csv"):
        f.unlink()


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def validate() -> bool:
    dataset_dir = ROOT / "data" / "datasets" / DATASET
    trans = f"{DATASET}_Trans.csv"
    log("Validating Q5 output...")
    env = {
        **os.environ,
        "Q5_DATASET_DIR": str(dataset_dir),
        "Q5_DATASET_TRANS": trans,
    }
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
    keep = args.keep

    if scenario == "smoke":
        target = f"{CONTAINER_PREFIX}_0"
        kill_trigger_id = "0"
    elif scenario == "A":
        target = f"{CONTAINER_PREFIX}_1"   # non-leader
        kill_trigger_id = "1"
    elif scenario == "B":
        target = f"{CONTAINER_PREFIX}_0"   # leader
        kill_trigger_id = "0"
    else:
        die(f"unknown scenario: {scenario}")

    kill_pattern = f"filter_q5_usd_start | id={kill_trigger_id}"

    log(f"=== Crash test: scenario={scenario} target={target} dataset={DATASET} ===")
    print()

    generate_compose(scenario)
    teardown(keep=False)
    clear_output()
    build_and_start()

    log(f"Waiting for '{kill_pattern}' in {target} logs...")
    watcher = LogWatcher(target, kill_pattern)
    watcher.start()

    if not watcher.wait_for_match(TRIGGER_TIMEOUT):
        teardown(keep)
        die(
            f"Timed out after {TRIGGER_TIMEOUT}s waiting for start log in {target}.\n"
            f"  Check 'docker logs {target}' for errors."
        )
    watcher.stop()

    log(f"Sleeping {KILL_DELAY_AFTER_START}s so {target} writes WAL entries...")
    time.sleep(KILL_DELAY_AFTER_START)

    log(f"Killing {target} with SIGKILL...")
    result = subprocess.run(["docker", "kill", target], capture_output=True, text=True)
    if result.returncode != 0:
        teardown(keep)
        die(f"docker kill failed: {result.stderr.strip()}")

    time.sleep(1)

    restart_container(target)

    try:
        finished = wait_for_client(CLIENT_TIMEOUT)
    except subprocess.TimeoutExpired:
        finished = False

    if not finished:
        log("WARNING: client_0 did not finish within timeout — dumping tail logs")
        run(compose(PROJECT, "logs", "--tail", "50", target))
        teardown(keep)
        die(f"client_0 did not finish after crash+restart of {target}")

    ok = validate()

    if not keep:
        teardown(keep=False)

    print()
    try:
        import ref
        if ok:
            print(ref.green(f"✓✓✓ CRASH TEST PASSED (scenario={scenario}) ✓✓✓"))
        else:
            print(ref.red(f"✗✗✗ CRASH TEST FAILED (scenario={scenario}) ✗✗✗"))
    except ImportError:
        status = "PASSED" if ok else "FAILED"
        print(f"{'✓✓✓' if ok else '✗✗✗'} CRASH TEST {status} (scenario={scenario})")

    sys.exit(0 if ok else 1)


def parse_args():
    p = argparse.ArgumentParser(description="Crash-recovery test for filter_q5_usd")
    p.add_argument("--scenario", choices=["smoke", "A", "B"], required=True)
    p.add_argument("--keep", action="store_true", help="Leave stack running after test")
    return p.parse_args()


if __name__ == "__main__":
    main()
