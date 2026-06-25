#!/usr/bin/env python3
"""Crash-recovery smoke test for the Q2 joiner pipeline.

Both workers sit between the aggregation_q2 output and the gateway, so killing
either one during DATA processing exercises WAL replay and addressed-packet
redelivery across the full Q2 path.

Scenarios
---------
  joiner   Kill join_q2 (the generic joiner) during DATA from aggregation_q2.
           Tests WAL replay of JoinerState (data/eof/close changes).

  bank     Kill q2_bank_name_joiner during DATA / accounts join.
           Tests WAL replay of the bank-name joiner state after addressed
           packets are redelivered from file_splitter and join_q2.

Usage
-----
  python scripts/crash_test_joiner.py --scenario joiner
  python scripts/crash_test_joiner.py --scenario bank
  python scripts/crash_test_joiner.py --scenario joiner --dataset LI-Mini --keep
"""

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

COMPOSE_FILE = ROOT / "tmp" / "crash-tests" / "docker-compose.joiner.yaml"

KILL_DELAY_AFTER_START = 10
TRIGGER_TIMEOUT = 120
CLIENT_TIMEOUT = 300

_SCENARIOS = {
    "joiner": {
        "container": "join_q2",
        "kill_pattern": "joiner_start | configuration=Q2 | id=0",
    },
    "bank": {
        "container": "q2_bank_name_joiner",
        "kill_pattern": "q2_bank_name_joiner_start | id=0",
    },
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def compose(project, *args):
    return [
        "docker",
        "compose",
        "--project-directory",
        str(ROOT),
        "-p",
        project,
        "-f",
        str(COMPOSE_FILE),
        *args,
    ]


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

def generate_compose(dataset: str):
    cmd = [
        sys.executable, str(ROOT / "scripts" / "generate_compose.py"),
        "--preset", "q2-test",
        "--dataset", dataset,
        "--test-output", str(COMPOSE_FILE),
        "--skip-output",
    ]
    log(f"Generating compose (q2-test, dataset={dataset})...")
    run(cmd, check=True)


def teardown(project):
    log("Tearing down stack...")
    quiet(compose(project, "down", "--volumes", "--remove-orphans"))
    ids = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"],
        capture_output=True, text=True,
    ).stdout.split()
    if ids:
        quiet(["docker", "rm", "-f", *ids])


def build_and_start(project):
    log("Building images and starting stack...")
    run(compose(project, "up", "--build", "--remove-orphans", "--detach"), check=True)


def restart_container(project, name: str):
    log(f"Restarting {name}...")
    run(compose(project, "up", "--detach", name), check=True)


def wait_for_client(project, timeout: int) -> bool:
    log(f"Waiting for client_0 (timeout={timeout}s)...")
    result = run(
        compose(project, "wait", "client_0"),
        timeout=timeout,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
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
    log("Validating Q2 output...")
    env = {
        **os.environ,
        "Q2_DATASET_DIR": str(dataset_dir),
        "Q2_DATASET_TRANS": trans,
    }
    result = run(
        [sys.executable, str(ROOT / "scripts" / "validate_q2_output.py")],
        env=env,
    )
    return result.returncode == 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    global COMPOSE_FILE

    args = parse_args()
    scenario = args.scenario
    dataset = args.dataset
    COMPOSE_FILE = resolve_compose_file(
        ROOT,
        args.compose_file,
        "joiner",
        scenario,
        dataset,
    )
    cfg = _SCENARIOS[scenario]
    target = cfg["container"]
    kill_pattern = cfg["kill_pattern"]
    project = "crash-test-joiner"

    log(f"=== Crash test: scenario={scenario} target={target} dataset={dataset} ===")
    print()

    generate_compose(dataset)
    teardown(project)
    clear_output()
    build_and_start(project)

    log(f"Waiting for '{kill_pattern}' in {target} logs...")
    watcher = LogWatcher(target, kill_pattern)
    watcher.start()

    if not watcher.wait_for_match(TRIGGER_TIMEOUT):
        teardown(project)
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
        teardown(project)
        die(f"docker kill failed: {result.stderr.strip()}")

    time.sleep(1)

    restart_container(project, target)

    try:
        finished = wait_for_client(project, CLIENT_TIMEOUT)
    except subprocess.TimeoutExpired:
        finished = False

    if not finished:
        log("WARNING: client_0 did not finish within timeout — dumping tail logs")
        run(compose(project, "logs", "--tail", "50", target))
        teardown(project)
        die(f"client_0 did not finish after crash+restart of {target}")

    ok = validate(dataset)

    if not args.keep:
        teardown(project)
    else:
        log(
            f"--keep set. Stack is up. Tear down with:\n"
            f"  docker compose -p {project} -f {COMPOSE_FILE} down --volumes --remove-orphans"
        )

    print()
    if ok:
        print(ref.green(f"✓✓✓ CRASH TEST PASSED (scenario={scenario}) ✓✓✓"))
        return 0
    else:
        print(ref.red(f"✗✗✗ CRASH TEST FAILED (scenario={scenario}) ✗✗✗"))
        return 1


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--scenario", choices=["joiner", "bank"], required=True,
        help="joiner=kill join_q2; bank=kill q2_bank_name_joiner",
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
    p.add_argument(
        "--compose-file",
        help="Path for the generated compose file (default: tmp/crash-tests/...).",
    )
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(main())
