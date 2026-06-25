#!/usr/bin/env python3
"""
ft_monitor.py — Fault Tolerance Protocol Visualizer

Watches Docker containers in real-time and renders:
  • Container health table (Running / Stopped / Recovering)
  • WAL + snapshot file sizes from /worker_state inside each container
  • Live protocol event feed, color-coded by phase

Usage:
  python scripts/ft_monitor.py [filter ...]

  Each filter is a substring matched against container names.
  With no filters, all running (or recently stopped) containers are shown
  EXCEPT RabbitMQ, monitor, client, gateway, and rates-service containers.

Examples:
  python scripts/ft_monitor.py                   # all workers
  python scripts/ft_monitor.py aggregation_q5    # only q5 aggregators
  python scripts/ft_monitor.py filter agg        # filters + aggregators

Kill a worker in another terminal:
  docker kill <container_name>
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime

# ─── ANSI palette ────────────────────────────────────────────────────────────

class C:
    RED    = "\033[91m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    BLUE   = "\033[94m"
    CYAN   = "\033[96m"
    MAGENTA= "\033[95m"
    WHITE  = "\033[97m"
    DIM    = "\033[2m"
    BOLD   = "\033[1m"
    RESET  = "\033[0m"

def colored(text: str, color: str) -> str:
    return f"{color}{text}{C.RESET}"

# ─── Protocol event rules ─────────────────────────────────────────────────────
# Each entry: (substring_to_match, label, color)
# The FIRST matching rule wins.

EVENT_RULES: list[tuple[str, str, str]] = [
    ("monitor_node_failed",      "FAILURE   ", C.RED),
    ("monitor_recovery_start",   "RECOV-INIT", C.YELLOW),
    ("monitor_recovery_success", "RECOVERED ", C.GREEN),
    ("monitor_recovery_failed",  "RECOV-FAIL", C.RED),
    ("worker_runner_recovered",  "WAL-REPLAY", C.CYAN),
    ("SNAPSHOT_TAKEN",           "SNAPSHOT  ", C.BLUE),
    ("monitor_leader_elected",   "ELECTION  ", C.MAGENTA),
    ("monitor_election_start",   "ELECTION  ", C.MAGENTA),
    ("Traceback",                "EXCEPTION ", C.RED),
]

# Containers whose logs we parse but that are NOT workers (no WAL/state files).
NON_WORKER_CONTAINERS = {"rabbitmq", "client", "gateway", "rates_service"}

# Default exclude patterns when no filter is provided.
DEFAULT_EXCLUDE = {"rabbitmq", "client_", "gateway", "rates_service"}

STATE_DIR = "/worker_state"

# ─── Shared state (lock-protected) ───────────────────────────────────────────

_lock = threading.Lock()
_events: deque[str] = deque(maxlen=200)
_container_meta: dict[str, dict] = {}   # name → {status, wal_size, snap_mtime, is_worker}


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _fmt_size(n: int | None) -> str:
    if n is None:
        return "—"
    if n < 1024:
        return f"{n} B"
    if n < 1024 ** 2:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024**2:.1f} MB"


def _fmt_age(ts: float | None) -> str:
    if ts is None:
        return "—"
    age = int(time.time() - ts)
    if age < 60:
        return f"{age}s ago"
    if age < 3600:
        return f"{age // 60}m {age % 60}s ago"
    return f"{age // 3600}h ago"


# ─── Docker helpers ───────────────────────────────────────────────────────────

def _docker(*args, timeout: float = 5.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def list_containers(filters: list[str]) -> list[str]:
    """Return container names matching any of the filters (or all workers if empty)."""
    r = _docker("ps", "-a", "--format", "{{.Names}}", "--filter", "status=running",
                "--filter", "status=exited", "--filter", "status=created")
    names = [n.strip() for n in r.stdout.splitlines() if n.strip()]

    if filters:
        return [n for n in names if any(f in n for f in filters)]

    # No filter: exclude infra containers.
    return [
        n for n in names
        if not any(excl in n for excl in DEFAULT_EXCLUDE)
    ]


def container_status(name: str) -> str:
    r = _docker("inspect", "--format", "{{.State.Status}}", name)
    return r.stdout.strip() or "unknown"


def file_stat(container: str, path: str) -> tuple[int | None, float | None]:
    """Return (size_bytes, mtime_epoch) of a file inside a container, or (None, None)."""
    r = _docker("exec", container, "stat", "-c", "%s %Y", path, timeout=3.0)
    if r.returncode != 0:
        return None, None
    parts = r.stdout.strip().split()
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), float(parts[1])
    except ValueError:
        return None, None


# ─── Background threads ───────────────────────────────────────────────────────

def _log_stream_thread(container: str, stop: threading.Event) -> None:
    """Stream docker logs and push matching lines to _events."""
    try:
        proc = subprocess.Popen(
            ["docker", "logs", "-f", "--timestamps", "--tail", "50", container],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except FileNotFoundError:
        return

    try:
        for raw_line in proc.stdout:
            if stop.is_set():
                break
            line = raw_line.strip()
            for pattern, label, color in EVENT_RULES:
                if pattern in line:
                    # Parse docker log timestamp prefix (2006-01-02T15:04:05.000Z)
                    parts = line.split(" ", 1)
                    ts_str = _now()
                    msg = parts[1] if len(parts) == 2 else line
                    # Trim the msg to a sane length
                    msg = msg[:120]
                    short_name = container.split("-")[-2] if "-" in container else container
                    short_name = short_name[:22]
                    entry = (
                        f"{colored(ts_str, C.DIM)}  "
                        f"[{colored(label, color)}]  "
                        f"{colored(short_name, C.WHITE):<22}  "
                        f"{msg}"
                    )
                    with _lock:
                        _events.appendleft(entry)
                    break
    finally:
        proc.terminate()


def _state_poll_thread(container: str, stop: threading.Event) -> None:
    """Poll container status and /worker_state file sizes every 3 s."""
    is_worker = not any(excl in container for excl in NON_WORKER_CONTAINERS)
    while not stop.is_set():
        status = container_status(container)

        wal_size = snap_mtime = None
        if is_worker and status == "running":
            wal_size, _ = file_stat(container, f"{STATE_DIR}/wal.current")
            _, snap_mtime = file_stat(container, f"{STATE_DIR}/last_state.current")

        with _lock:
            _container_meta[container] = {
                "status": status,
                "wal_size": wal_size,
                "snap_mtime": snap_mtime,
                "is_worker": is_worker,
            }
        stop.wait(3.0)


# ─── Rendering ────────────────────────────────────────────────────────────────

_STATUS_COLOR = {
    "running": C.GREEN,
    "exited":  C.RED,
    "created": C.YELLOW,
    "paused":  C.YELLOW,
    "unknown": C.DIM,
}

_STATUS_ICON = {
    "running": "●",
    "exited":  "✗",
    "created": "○",
    "paused":  "⏸",
    "unknown": "?",
}


def _render(terminal_width: int, event_rows: int) -> str:
    lines: list[str] = []

    # ── Header ──
    title = "  FAULT TOLERANCE PROTOCOL MONITOR"
    ts = f"{_now()}  "
    pad = max(0, terminal_width - len(title) - len(ts))
    lines.append(colored(f"{title}{' ' * pad}{ts}", C.BOLD + C.WHITE))
    lines.append(colored("─" * terminal_width, C.DIM))

    # ── Container table ──
    col_name  = 26
    col_state = 14
    col_wal   = 10
    col_snap  = 24

    header = (
        f"  {'Worker':<{col_name}}  {'Status':<{col_state}}  "
        f"{'WAL':<{col_wal}}  {'Last Snapshot':<{col_snap}}"
    )
    lines.append(colored(header, C.BOLD))
    lines.append(colored("  " + "─" * (col_name + col_state + col_wal + col_snap + 8), C.DIM))

    with _lock:
        meta_snapshot = dict(_container_meta)

    if not meta_snapshot:
        lines.append(colored("  (no containers discovered yet)", C.DIM))
    else:
        for name in sorted(meta_snapshot):
            m = meta_snapshot[name]
            status = m["status"]
            color  = _STATUS_COLOR.get(status, C.DIM)
            icon   = _STATUS_ICON.get(status, "?")

            short = name
            # Strip compose project prefix (e.g. "distribuidos-filter_usd_0-1" → "filter_usd_0")
            parts = name.split("-")
            if len(parts) >= 3 and parts[-1].isdigit():
                short = "-".join(parts[1:-1])
            short = short[:col_name]

            status_str = colored(f"{icon} {status.upper():<{col_state - 2}}", color)
            wal_str    = colored(f"{_fmt_size(m['wal_size']):<{col_wal}}", C.CYAN if m["wal_size"] else C.DIM)
            snap_str   = colored(f"{_fmt_age(m['snap_mtime']):<{col_snap}}", C.BLUE if m["snap_mtime"] else C.DIM)

            lines.append(
                f"  {short:<{col_name}}  {status_str}  {wal_str}  {snap_str}"
            )

    lines.append("")
    lines.append(colored("── Protocol Events " + "─" * max(0, terminal_width - 19), C.DIM))

    with _lock:
        recent = list(_events)[:event_rows]

    if not recent:
        lines.append(colored("  (waiting for events — kill a worker in another terminal)", C.DIM))
    else:
        for ev in recent:
            lines.append("  " + ev)

    return "\n".join(lines)


# ─── Main loop ────────────────────────────────────────────────────────────────

def _terminal_size() -> tuple[int, int]:
    try:
        sz = os.get_terminal_size()
        return sz.columns, sz.lines
    except OSError:
        return 120, 40


def main() -> None:
    filters = sys.argv[1:]
    print(colored("Discovering containers…", C.DIM), flush=True)
    containers = list_containers(filters)

    if not containers:
        print(colored("No matching containers found. Is the stack running?", C.RED))
        sys.exit(1)

    print(colored(f"Monitoring {len(containers)} container(s): {', '.join(containers)}", C.DIM))
    time.sleep(0.5)

    stop_event = threading.Event()
    threads: list[threading.Thread] = []

    for name in containers:
        for target in (_log_stream_thread, _state_poll_thread):
            t = threading.Thread(target=target, args=(name, stop_event), daemon=True)
            t.start()
            threads.append(t)

    try:
        while True:
            width, height = _terminal_size()
            # Reserve rows: header(1) + divider(1) + table header(2) + containers + 2 blank + events header
            reserved = 1 + 1 + 2 + len(containers) + 3
            event_rows = max(5, height - reserved)

            frame = _render(width, event_rows)

            # Move cursor to top-left and overwrite
            sys.stdout.write("\033[H\033[J")
            sys.stdout.write(frame)
            sys.stdout.flush()
            time.sleep(1.0)

    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        print(colored("\n\nMonitor stopped.", C.DIM))


if __name__ == "__main__":
    main()
