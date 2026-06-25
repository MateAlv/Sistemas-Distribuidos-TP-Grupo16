#!/usr/bin/env python3
"""Abort-propagation smoke test: verifies client-disconnect cleanup.

When a client disconnects abruptly mid-upload the gateway must broadcast
MSG_ABORT to every file-splitter partition so downstream workers can discard
orphaned per-client state and free resources.

This test kills the client_0 container with SIGKILL (which drops the TCP
connection without FIN, as a hard crash does), then verifies that the abort
tombstone propagates through the first two pipeline stages:

  1. ``gateway_abort_sent``    — gateway dispatched the abort.
  2. ``file_splitter_abort``   — file_splitter received and WAL-tracked it.
  3. ``file_ingestor_abort``   — file_ingestor received and propagated it.
  4. No infrastructure container exited unexpectedly after the event.

Usage
-----
  python scripts/crash_test_abort.py
  python scripts/crash_test_abort.py --dataset LI-Small --keep
  python scripts/crash_test_abort.py --compose-file tmp/crash-tests/docker-compose.abort.yaml
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from crash_test_paths import resolve_compose_file  # noqa: E402
import reference_results as ref  # noqa: E402

PROJECT = "crash-test-abort"
DEFAULT_DATASET = "LI-Mini"

# Seconds to wait for the first file-splitter chunk before giving up.
UPLOAD_START_TIMEOUT = 90
# Extra seconds after first chunk so non-trivial state accumulates in workers.
KILL_DELAY = 5
# Seconds to wait for abort to appear in gateway logs.
ABORT_GATEWAY_TIMEOUT = 30
# Seconds to wait for abort to appear in each worker's logs.
ABORT_WORKER_TIMEOUT = 60


def compose(*args):
    return [
        "docker", "compose",
        "--project-directory", str(ROOT),
        "-p", PROJECT,
        "-f", str(COMPOSE_FILE),
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


# ---------------------------------------------------------------------------
# multi-pattern log watcher
# ---------------------------------------------------------------------------

class MultiLogWatcher(threading.Thread):
    """Streams ``docker compose logs -f`` and fires a per-pattern event on match."""

    def __init__(self, patterns: list[str]) -> None:
        super().__init__(daemon=True)
        self._events: dict[str, threading.Event] = {p: threading.Event() for p in patterns}
        self._stop_requested = threading.Event()
        self._proc: subprocess.Popen | None = None

    def run(self) -> None:
        self._proc = subprocess.Popen(
            compose("logs", "--follow", "--no-color", "--tail", "200"),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert self._proc.stdout is not None
        try:
            for line in self._proc.stdout:
                if self._stop_requested.is_set():
                    break
                for pattern, event in self._events.items():
                    if not event.is_set() and pattern in line:
                        event.set()
        finally:
            self._proc.terminate()

    def wait_for(self, pattern: str, timeout: float) -> bool:
        return self._events[pattern].wait(timeout=timeout)

    def stop(self) -> None:
        self._stop_requested.set()
        if self._proc is not None:
            self._proc.terminate()


# ---------------------------------------------------------------------------
# lifecycle helpers
# ---------------------------------------------------------------------------

def generate_compose(dataset: str) -> None:
    cmd = [
        sys.executable, str(ROOT / "scripts" / "generate_compose.py"),
        "--preset", "q1-test",
        "--dataset", dataset,
        "--test-output", str(COMPOSE_FILE),
        "--skip-output",
    ]
    log(f"Generating compose (q1-test, dataset={dataset})...")
    run(cmd, check=True)


def build_and_start() -> None:
    log("Building images and starting stack...")
    run(compose("up", "--build", "--remove-orphans", "--detach"), check=True)


def teardown(keep: bool) -> None:
    if keep:
        log(
            "--keep set: stack is still running.\n"
            f"  Tear down: docker compose -p {PROJECT} -f {COMPOSE_FILE} "
            "down --volumes --remove-orphans"
        )
        return
    log("Tearing down stack...")
    quiet(compose("down", "--volumes", "--remove-orphans"))
    ids = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}"],
        capture_output=True, text=True,
    ).stdout.split()
    if ids:
        quiet(["docker", "rm", "-f", *ids])


def kill_client() -> bool:
    """SIGKILL client_0 — simulates an abrupt TCP drop (no FIN)."""
    log("Killing client_0 with SIGKILL (simulating hard disconnect)...")
    result = subprocess.run(
        compose("kill", "client_0"),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log(f"WARNING: docker compose kill failed: {result.stderr.strip()}")
        return False
    return True


def check_no_unexpected_exits() -> bool:
    """Return True if only client_0 has exited (all workers still running)."""
    result = subprocess.run(
        compose("ps", "--status", "exited"),
        capture_output=True, text=True,
    )
    # Filter out the header line and the expected killed client.
    unexpected = [
        line for line in result.stdout.splitlines()
        if line.strip() and "NAME" not in line and "client_0" not in line
    ]
    if unexpected:
        log("WARNING: unexpected exited containers:")
        for line in unexpected:
            log(f"  {line}")
        return False
    return True


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    global COMPOSE_FILE

    args = parse_args()
    dataset = args.dataset
    COMPOSE_FILE = resolve_compose_file(ROOT, args.compose_file, "abort", dataset)

    log(f"=== Abort propagation test (dataset={dataset}) ===")
    print()

    generate_compose(dataset)
    teardown(keep=False)
    build_and_start()

    watcher = MultiLogWatcher([
        "file_splitter_chunk",
        "gateway_abort_sent",
        "file_splitter_abort",
        "file_ingestor_abort",
    ])
    watcher.start()

    # ---- wait for client to start uploading ----
    log(f"Waiting for first file_splitter_chunk (timeout={UPLOAD_START_TIMEOUT}s)...")
    if not watcher.wait_for("file_splitter_chunk", UPLOAD_START_TIMEOUT):
        watcher.stop()
        teardown(args.keep)
        die(
            f"Timed out after {UPLOAD_START_TIMEOUT}s — no file_splitter_chunk seen.\n"
            "  Check: docker compose logs (look for startup errors)."
        )

    log(f"Upload in progress. Sleeping {KILL_DELAY}s to let worker state accumulate...")
    time.sleep(KILL_DELAY)

    # ---- kill the client (simulate hard disconnect) ----
    if not kill_client():
        watcher.stop()
        teardown(args.keep)
        die("Could not kill client_0.")

    # ---- verify abort propagation ----
    log(f"Waiting for gateway_abort_sent (timeout={ABORT_GATEWAY_TIMEOUT}s)...")
    gateway_ok = watcher.wait_for("gateway_abort_sent", ABORT_GATEWAY_TIMEOUT)
    if not gateway_ok:
        log(ref.red("  FAIL: gateway did not log gateway_abort_sent within timeout."))
    else:
        log(ref.green("  OK: gateway_abort_sent seen."))

    log(f"Waiting for file_splitter_abort (timeout={ABORT_WORKER_TIMEOUT}s)...")
    splitter_ok = watcher.wait_for("file_splitter_abort", ABORT_WORKER_TIMEOUT)
    if not splitter_ok:
        log(ref.red("  FAIL: file_splitter did not log file_splitter_abort within timeout."))
    else:
        log(ref.green("  OK: file_splitter_abort seen."))

    log(f"Waiting for file_ingestor_abort (timeout={ABORT_WORKER_TIMEOUT}s)...")
    ingestor_ok = watcher.wait_for("file_ingestor_abort", ABORT_WORKER_TIMEOUT)
    if not ingestor_ok:
        log(ref.red("  FAIL: file_ingestor did not log file_ingestor_abort within timeout."))
    else:
        log(ref.green("  OK: file_ingestor_abort seen."))

    watcher.stop()

    # ---- confirm no infrastructure worker crashed ----
    time.sleep(2)
    no_crashes = check_no_unexpected_exits()
    if not no_crashes:
        log(ref.red("  FAIL: one or more infrastructure containers exited unexpectedly."))
    else:
        log(ref.green("  OK: no unexpected container exits."))

    teardown(args.keep)

    ok = gateway_ok and splitter_ok and ingestor_ok and no_crashes
    print()
    print("Results:")
    for label, passed in [
        ("gateway_abort_sent dispatched", gateway_ok),
        ("file_splitter_abort received ", splitter_ok),
        ("file_ingestor_abort received ", ingestor_ok),
        ("no unexpected worker exits   ", no_crashes),
    ]:
        cell = ref.green("✓") if passed else ref.red("✗")
        print(f"  {cell}  {label}")
    print()
    if ok:
        print(ref.green("✓✓✓ ABORT PROPAGATION TEST PASSED ✓✓✓"))
        return 0
    print(ref.red("✗✗✗ ABORT PROPAGATION TEST FAILED ✗✗✗"))
    return 1


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        choices=["LI-Mini", "LI-Small"],
        help=f"Dataset to use (default: {DEFAULT_DATASET})",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        default=False,
        help="Leave the stack up after the test (for log inspection)",
    )
    parser.add_argument(
        "--compose-file",
        help="Path for the generated compose file (default: tmp/crash-tests/...).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
