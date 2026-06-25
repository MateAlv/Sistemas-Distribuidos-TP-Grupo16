#!/usr/bin/env python3
"""Run the whole pipeline (Q1-Q5), validate every query's output per client
against the precomputed reference, and print a metrics footer (per-query
PASS/time, container count, peak CPU/RAM). Configured via env set by the
Makefile test target."""
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reference_results as ref

ROOT = Path(__file__).resolve().parents[1]
PRETTY_LOGS = ROOT / "scripts" / "pretty_logs.py"

LOG_COLOR = os.environ.get("LOG_COLOR", "always")
TEST_PROJECT = os.environ.get("TEST_PROJECT", "distribuidos-test")
MAIN_PROJECT = os.environ.get("MAIN_PROJECT", Path.cwd().name.lower())
COMPOSE_FILE = os.environ.get("TEST_COMPOSE_FILE", "docker-compose.test.yaml")
WAIT_TIMEOUT = os.environ.get("TEST_CLIENT_WAIT_TIMEOUT", "600s")
KEEP_CONTAINERS = bool(os.environ.get("KEEP_CONTAINERS", "").strip())
METRICS_INTERVAL = float(os.environ.get("METRICS_INTERVAL", "2"))

QUERIES = list(ref.QUERIES)
# gateway_eof line prefixes per query (gateway logs prefix or "Q1"); see
# src/gateway/gateway.py _run_result_consumer.
EOF_PATTERNS = {
    "q1": "gateway_eof | prefix=Q1 |",
    "q2": "gateway_eof | prefix=Q2| |",
    "q3": "gateway_eof | prefix=Q3| |",
    "q4": "gateway_eof | prefix=Q4| |",
    "q5": "gateway_eof | prefix=Q5| |",
}
_CLIENT_ID_RE = re.compile(r"client_id=(\d+)")

BAR = "=" * 60


@dataclass(frozen=True)
class ClientInput:
    client_id: int
    dataset_dir: Path
    trans_name: str


def compose(*args):
    return ["docker", "compose", "-p", TEST_PROJECT, "-f", COMPOSE_FILE, *args]


def run(cmd, **kwargs):
    return subprocess.run(cmd, **kwargs)


def quiet(cmd):
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# --------------------------------------------------------------------------- #
# compose client inputs
# --------------------------------------------------------------------------- #
def _compose_path():
    path = Path(COMPOSE_FILE)
    if path.is_absolute():
        return path
    return ROOT / path


def _env_dict(environment):
    if isinstance(environment, dict):
        return {str(k): str(v) for k, v in environment.items()}
    if isinstance(environment, list):
        env = {}
        for item in environment:
            if isinstance(item, str) and "=" in item:
                key, value = item.split("=", 1)
                env[key] = value
        return env
    return {}


def _volume_source_for_target(volumes, target):
    target = target.rstrip("/")
    for volume in volumes or []:
        if isinstance(volume, str):
            parts = volume.split(":")
            if len(parts) >= 2 and parts[1].rstrip("/") == target:
                return parts[0]
        elif isinstance(volume, dict):
            volume_target = str(volume.get("target", "")).rstrip("/")
            if volume_target == target:
                return volume.get("source")
    return None


def _resolve_compose_path(path, compose_path):
    path = Path(path)
    if path.is_absolute():
        return path
    return (compose_path.parent / path).resolve()


def _client_id(service_name, env):
    raw = env.get("CLIENT_ID")
    if raw is None:
        raw = service_name.rsplit("_", 1)[-1]
    return int(raw)


def load_client_inputs():
    compose_path = _compose_path()
    if not compose_path.exists():
        raise FileNotFoundError(f"compose file not found: {compose_path}")
    with compose_path.open("r", encoding="utf-8") as file:
        compose_doc = yaml.safe_load(file) or {}

    services = compose_doc.get("services", {})
    if not isinstance(services, dict):
        raise ValueError(f"{compose_path}: services must be a mapping")

    clients = []
    for service_name, service in sorted(services.items()):
        if not service_name.startswith("client_"):
            continue
        service = service or {}
        env = _env_dict(service.get("environment", {}))
        trans_name = env.get("TRANSACTIONS_FILE")
        if not trans_name:
            raise ValueError(f"{compose_path}: {service_name} missing TRANSACTIONS_FILE")
        data_dir = env.get("DATA_DIR", "/data/input")
        mount_source = _volume_source_for_target(service.get("volumes", []), data_dir)
        if not mount_source:
            raise ValueError(
                f"{compose_path}: {service_name} has no host volume mounted at {data_dir}"
            )

        trans_path = Path(trans_name)
        dataset_dir = _resolve_compose_path(mount_source, compose_path) / trans_path.parent
        clients.append(ClientInput(
            client_id=_client_id(service_name, env),
            dataset_dir=dataset_dir.resolve(),
            trans_name=trans_path.name,
        ))

    if not clients:
        raise ValueError(f"{compose_path}: no client_* services found")
    return sorted(clients, key=lambda client: client.client_id)


def _input_key(client_input):
    return (client_input.dataset_dir, client_input.trans_name)


def _input_label(client_input):
    try:
        dataset_dir = client_input.dataset_dir.relative_to(ROOT)
    except ValueError:
        dataset_dir = client_input.dataset_dir
    return f"{dataset_dir}/{client_input.trans_name}"


def dataset_summary(client_inputs):
    unique = []
    seen = set()
    for client_input in client_inputs:
        key = _input_key(client_input)
        if key in seen:
            continue
        seen.add(key)
        unique.append(client_input)
    if len(unique) == 1:
        return _input_label(unique[0])
    return ", ".join(
        f"client {client_input.client_id}: {_input_label(client_input)}"
        for client_input in client_inputs
    )


# --------------------------------------------------------------------------- #
# cleanup / teardown
# --------------------------------------------------------------------------- #
def kill_project(project):
    """Force-remove containers and networks for a compose project by label."""
    label = f"label=com.docker.compose.project={project}"
    ids = subprocess.run(
        ["docker", "ps", "-aq", "--filter", label],
        capture_output=True, text=True,
    ).stdout.split()
    if ids:
        quiet(["docker", "rm", "-f", *ids])
    nets = subprocess.run(
        ["docker", "network", "ls", "-q", "--filter", label],
        capture_output=True, text=True,
    ).stdout.split()
    if nets:
        quiet(["docker", "network", "rm", *nets])


def teardown():
    quiet(compose("down", "--volumes", "--remove-orphans"))
    kill_project(TEST_PROJECT)


def cleanup_all():
    print("Killing leftover TP containers (main + test projects)...")
    teardown()
    kill_project(MAIN_PROJECT)
    out = Path("data/output")
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("results_q*.csv"):
        f.unlink()


# --------------------------------------------------------------------------- #
# metrics sampler
# --------------------------------------------------------------------------- #
_MEM_UNITS = {
    "B": 1.0 / (1024 * 1024),
    "KB": 1.0 / 1024, "KIB": 1.0 / 1024,
    "MB": 1.0, "MIB": 1.0,
    "GB": 1024.0, "GIB": 1024.0,
    "TB": 1024.0 * 1024, "TIB": 1024.0 * 1024,
}


def _mem_to_mib(text):
    m = re.match(r"\s*([0-9.]+)\s*([A-Za-z]+)", text)
    if not m:
        return 0.0
    return float(m.group(1)) * _MEM_UNITS.get(m.group(2).upper(), 1.0)


class MetricsSampler(threading.Thread):
    def __init__(self, project, interval):
        super().__init__(daemon=True)
        self._project = project + "-"
        self._interval = interval
        self._stopev = threading.Event()
        self.peak_cpu_by_container = defaultdict(float)
        self.peak_mem_by_container = defaultdict(float)
        self.peak_total_cpu = 0.0
        self.peak_total_mem = 0.0
        self.containers = set()

    def stop(self):
        self._stopev.set()

    def run(self):
        fmt = "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
        while not self._stopev.is_set():
            proc = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", fmt],
                capture_output=True, text=True,
            )
            total_cpu = 0.0
            total_mem = 0.0
            for line in proc.stdout.splitlines():
                parts = line.split("\t")
                if len(parts) != 3:
                    continue
                name, cpu_s, mem_s = parts
                if not name.startswith(self._project):
                    continue
                try:
                    cpu = float(cpu_s.replace("%", "").strip())
                except ValueError:
                    continue
                mem = _mem_to_mib(mem_s.split("/")[0])
                self.containers.add(name)
                total_cpu += cpu
                total_mem += mem
                self.peak_cpu_by_container[name] = max(
                    self.peak_cpu_by_container[name], cpu)
                self.peak_mem_by_container[name] = max(
                    self.peak_mem_by_container[name], mem)
            self.peak_total_cpu = max(self.peak_total_cpu, total_cpu)
            self.peak_total_mem = max(self.peak_total_mem, total_mem)
            self._stopev.wait(self._interval)


# --------------------------------------------------------------------------- #
# per-query timing watcher (tails the tee'd raw log)
# --------------------------------------------------------------------------- #
class TimingWatcher(threading.Thread):
    def __init__(self, log_file, num_clients, start):
        super().__init__(daemon=True)
        self._log_file = log_file
        self._num_clients = num_clients
        self._start = start
        self._stopev = threading.Event()
        self.done_at = {}
        self._seen = {q: set() for q in QUERIES}

    def stop(self):
        self._stopev.set()

    def _scan(self, text):
        for line in text.splitlines():
            for q, pat in EOF_PATTERNS.items():
                if q in self.done_at or pat not in line:
                    continue
                m = _CLIENT_ID_RE.search(line)
                self._seen[q].add(m.group(1) if m else line)
                if len(self._seen[q]) >= self._num_clients:
                    self.done_at[q] = time.monotonic() - self._start

    def run(self):
        pos = 0
        while not self._stopev.is_set():
            try:
                with open(self._log_file, "r", errors="replace") as f:
                    f.seek(pos)
                    chunk = f.read()
                    pos = f.tell()
            except FileNotFoundError:
                chunk = ""
            if chunk:
                self._scan(chunk)
            self._stopev.wait(0.5)
        # final pass to catch anything written just before stop
        try:
            with open(self._log_file, "r", errors="replace") as f:
                f.seek(pos)
                self._scan(f.read())
        except FileNotFoundError:
            pass


# --------------------------------------------------------------------------- #
# validation
# --------------------------------------------------------------------------- #
def ensure_reference(client_inputs):
    ok = True
    seen = set()
    for client_input in client_inputs:
        key = _input_key(client_input)
        if key in seen:
            continue
        seen.add(key)
        missing = [
            q for q in QUERIES
            if not ref.expected_path(client_input.dataset_dir, q).exists()
        ]
        if not missing:
            continue
        print(
            f"Reference results missing for {missing}; "
            f"precomputing for {_input_label(client_input)}..."
        )
        proc = run([
            sys.executable,
            str(ROOT / "scripts" / "precompute_expected.py"),
            "--dataset", client_input.dataset_dir.name,
            "--dataset-root", str(client_input.dataset_dir.parent),
            "--trans", client_input.trans_name,
            "--queries", ",".join(missing),
        ])
        ok = ok and proc.returncode == 0
    return ok


def _print_counter_examples(label, counter):
    for row in list(counter)[:5]:
        print(f"      {label}: {row}")


def validate_query_for_clients(query, client_inputs, aborted_clients=frozenset(),
                               output_dir="data/output"):
    print("=" * 60)
    print(f"{query.upper()} FLOW VALIDATION")
    print("=" * 60)
    all_ok = True
    for client_input in client_inputs:
        print(f"\n  Client {client_input.client_id}: {_input_label(client_input)}")
        if client_input.client_id in aborted_clients:
            print(ref.yellow(
                "    ↯ disconnected mid-query by the monkey — ABORT expected, "
                "results intentionally not produced; skipping validation"
            ))
            continue
        try:
            expected = ref.expected_counter(
                query,
                client_input.dataset_dir,
                client_input.trans_name,
            )
        except Exception as e:  # noqa: BLE001
            print(f"    ERROR computing/loading expected {query} rows: {e}")
            all_ok = False
            continue

        src = ref.expected_path(client_input.dataset_dir, query)
        print(f"    Reference: {src if src.exists() else 'computed from dataset'}")
        print(f"    {ref._describe(query, expected)}")

        output_file = Path(output_dir) / f"results_{query}_{client_input.client_id}.csv"
        if not output_file.exists():
            print(f"    ERROR: missing output file {output_file}")
            all_ok = False
            continue

        try:
            actual = ref.load_counter(query, output_file)
        except Exception as e:  # noqa: BLE001
            print(f"    ERROR reading {output_file.name}: {e}")
            all_ok = False
            continue

        print(f"    Actual: {ref._summarize_actual(query, actual)}")
        missing, unexpected = ref.compare(expected, actual)
        if missing or unexpected:
            print(ref.red(
                f"    ERROR: differs from reference "
                f"(missing={sum(missing.values())}, "
                f"unexpected={sum(unexpected.values())})"
            ))
            _print_counter_examples("missing", missing)
            _print_counter_examples("unexpected", unexpected)
            all_ok = False
        else:
            print(ref.green("    ✓ matches reference"))

    print("=" * 60)
    if all_ok:
        print(ref.green(f"✓✓✓ {query.upper()} TEST PASSED ✓✓✓"))
    else:
        print(ref.red(f"✗✗✗ {query.upper()} TEST FAILED ✗✗✗"))
    return all_ok


def validate_all(client_inputs, aborted_clients=frozenset()):
    results = {}
    for q in QUERIES:
        results[q] = validate_query_for_clients(q, client_inputs, aborted_clients)
    return results


# --------------------------------------------------------------------------- #
# monkey footer
# --------------------------------------------------------------------------- #
_MONKEY_TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})")
_MONKEY_KILL_RE = re.compile(
    r"kill_container \| target: (\S+) \| role: (\S+) \| status: success")
_MONKEY_OK_RE = re.compile(r"monitor_recovery_success \| node_id=(\S+)")
_MONKEY_FAIL_RE = re.compile(r"monitor_recovery_failed \| node_id=(\S+)")
_ELECT_FAIL_RE = re.compile(r"monitor_node_failed \|.*node_id=(monitor_\d+).*is_leader=(\w+)")
_ELECT_START_RE = re.compile(r"monitor_election_started \| monitor_id=(\d+) \| epoch=(\d+)")
_ELECT_WON_RE = re.compile(r"monitor_election_won \| monitor_id=(\d+) \| epoch=(\d+)")
# Client disconnect / abort propagation.
_CLIENT_KILL_RE = re.compile(
    r"kill_container \| target: \S*client_(\d+)\S* \| role: client \| status: success")
_GATEWAY_ABORT_RE = re.compile(r"gateway_abort_sent \| client_id=(\d+)")
# Any worker stage flushing per-client state on abort: "<stage>_abort | ... client_id=N".
# The trailing " |" keeps the "*_abort_error" diagnostics out (they read
# "<stage>_abort_error |", which has no "<stage>_abort |" substring).
_STAGE_ABORT_RE = re.compile(r"(\w+_abort) \|.*?client_id=(\d+)")
# Workers on the worker_runner path (generic filter, q4_sum) log a uniform
# receipt line instead of a "<stage>_abort" line: derive the stage from node.
_RUNNER_ABORT_RE = re.compile(
    r"worker_runner_abort_received \| node=(\S+) \| client_id=(\d+)")
# Pipeline order used to present the per-stage flush timeline.
_ABORT_STAGE_ORDER = [
    "file_ingestor_abort",
    "file_splitter_abort",
    "filter_usd_abort",
    "filter_q1_abort",
    "filter_date_abort",
    "filter_q5_format_abort",
    "filter_q5_usd_abort",
    "sum_abort",
    "aggregation_abort",
    "joiner_abort",
    "q2_bank_name_joiner_abort",
    "q3_barrier_abort",
    "q4_filter_abort",
    "q4_joiner_abort",
    "q4_sum_abort",
    "q4_aggregator_abort",
    "q4_deduper_abort",
]


def _monkey_dt(line):
    m = _MONKEY_TS_RE.search(line)
    if not m:
        return None, "        "
    return datetime.strptime(f"{m.group(1)}T{m.group(2)}", "%Y-%m-%dT%H:%M:%S"), m.group(2)


def print_election_summary(log_path):
    """Parse the monitor failover election triggered by killing the leader and
    print a timeline above the Monkey Kill Count."""
    leader_kill = None
    events = []  # (dt, hms, text, is_key)
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                dt, hms = _monkey_dt(line)
                m = _MONKEY_KILL_RE.search(line)
                if m and m.group(2) == "monitor_leader" and leader_kill is None:
                    leader_kill = (dt, hms, m.group(1))
                    continue
                m = _ELECT_FAIL_RE.search(line)
                if m and m.group(2).lower() == "true":
                    events.append((dt, hms,
                                   f"{m.group(1)} (leader) detected down → failover", False))
                    continue
                m = _ELECT_START_RE.search(line)
                if m:
                    events.append((dt, hms,
                                   f"election started by monitor_{m.group(1)} (epoch {m.group(2)})", False))
                    continue
                m = _ELECT_WON_RE.search(line)
                if m:
                    events.append((dt, hms,
                                   f"monitor_{m.group(1)} WON election (epoch {m.group(2)}) → new leader", True))
                    continue
                m = _MONKEY_OK_RE.search(line)
                if m and m.group(1).startswith("monitor_"):
                    events.append((dt, hms,
                                   f"{m.group(1)} revived (rejoins as follower)", False))

    except OSError:
        return

    print()
    print(BAR)
    print("Monitor Election (leader failover)")
    print(BAR)
    if leader_kill is None:
        print("Monitor leader was not killed this run — no failover election.")
        print(BAR)
        return

    kdt, khms, leader = leader_kill
    print(f"Monkey killed monitor leader: {leader} at {khms}")
    print()
    # Only events at/after the leader kill, deduplicated, in time order.
    seen, won = set(), False
    for dt, hms, text, key in sorted(events, key=lambda e: e[0] or datetime.min):
        if kdt is not None and dt is not None and dt < kdt:
            continue
        if text in seen:
            continue
        seen.add(text)
        won = won or key
        out = f"  {hms}  {text}"
        print(ref.green(out) if key else out)
    print()
    print(ref.green("Result: new leader elected ✓") if won
          else ref.red("Result: no new leader observed ✗"))
    print(BAR)


def print_monkey_summary(log_path):
    """Parse the run log for monkey kills and monitor revivals and print a
    'Monkey Kill Count' table above the golden pipeline summary."""
    kills, revives_ok, revives_fail = [], [], []
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                dt, hms = _monkey_dt(line)
                m = _MONKEY_KILL_RE.search(line)
                if m:
                    kills.append((dt, hms, m.group(1), m.group(2)))
                    continue
                m = _MONKEY_OK_RE.search(line)
                if m:
                    revives_ok.append((dt, hms, m.group(1)))
                    continue
                m = _MONKEY_FAIL_RE.search(line)
                if m:
                    revives_fail.append((dt, hms, m.group(1)))
    except OSError:
        return

    print()
    print(BAR)
    print("Monkey Kill Count and its results")
    print(BAR)
    if not kills:
        print("Kills: 0 — no monkey kills during this run "
              "(it may have finished before the kill interval).")
        print(BAR)
        return

    used_ok, used_fail, rows = set(), set(), []
    revived = not_revived = 0
    for kdt, khms, container, role in sorted(kills, key=lambda k: k[0] or datetime.min):
        revival = None
        for i, (rdt, rhms, rc) in enumerate(revives_ok):
            if i in used_ok or rc != container:
                continue
            if kdt is None or rdt is None or rdt >= kdt:
                revival = (i, rdt, rhms)
                break
        if revival is not None:
            i, rdt, rhms = revival
            used_ok.add(i)
            if kdt and rdt:
                recov = f"+{(rdt - kdt).total_seconds():.0f}s via monitor (docker start)"
            else:
                recov = "via monitor (docker start)"
            rows.append((container, role, khms, rhms, recov, True))
            revived += 1
            continue
        failed = None
        for i, (rdt, rhms, rc) in enumerate(revives_fail):
            if i in used_fail or rc != container:
                continue
            if kdt is None or rdt is None or rdt >= kdt:
                failed = (i, rhms)
                break
        if failed is not None:
            used_fail.add(failed[0])
            rows.append((container, role, khms, failed[1], "recovery FAILED (monitor)", False))
        else:
            rows.append((container, role, khms, "—", "NOT REVIVED", False))
        not_revived += 1

    print(f"Kills: {len(kills)}   Revived: {revived}   Not revived: {not_revived}")
    print()
    print(f"{'Container':<22}{'Role':<16}{'Died':<10}{'Revived':<10}{'Recovery':<34}")
    for container, role, died, revived_at, recov, ok in rows:
        line = f"{container:<22}{role:<16}{died:<10}{revived_at:<10}{recov:<34}"
        print(ref.green(line) if ok else ref.red(line))
    print(BAR)


def aborted_clients_from_log(log_path):
    """Return the set of client_ids the monkey disconnected mid-query."""
    aborted = set()
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                m = _CLIENT_KILL_RE.search(line)
                if m:
                    aborted.add(int(m.group(1)))
    except OSError:
        pass
    return aborted


def print_abort_summary(log_path, aborted_clients):
    """For each monkey-disconnected client, show that the gateway broadcast the
    ABORT and that each pipeline stage flushed that client's per-client state."""
    if not aborted_clients:
        return

    gateway_abort = {}          # client_id -> hms
    stage_hits = {}             # client_id -> {stage_event -> (dt, hms)}
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                dt, hms = _monkey_dt(line)
                m = _GATEWAY_ABORT_RE.search(line)
                if m:
                    cid = int(m.group(1))
                    gateway_abort.setdefault(cid, hms)
                    continue
                m = _STAGE_ABORT_RE.search(line)
                if m:
                    cid = int(m.group(2))
                    seen = stage_hits.setdefault(cid, {})
                    seen.setdefault(m.group(1), (dt, hms))
                    continue
                m = _RUNNER_ABORT_RE.search(line)
                if m:
                    cid = int(m.group(2))
                    stage = re.sub(r"_\d+$", "", m.group(1)) + "_abort"
                    seen = stage_hits.setdefault(cid, {})
                    seen.setdefault(stage, (dt, hms))
    except OSError:
        return

    print()
    print(BAR)
    print("Client Disconnect / ABORT Cleanup")
    print(BAR)
    overall_ok = True
    for cid in sorted(aborted_clients):
        print(f"Client {cid}: disconnected mid-query by the monkey")
        gw = gateway_abort.get(cid)
        if gw:
            print(ref.green(f"  {gw}  gateway broadcast ABORT to file_ingestor partitions"))
        else:
            print(ref.red(
                "  —        gateway did NOT broadcast ABORT "
                "(client likely finished uploading before being killed)"))
            overall_ok = False

        seen = stage_hits.get(cid, {})
        ordered = [s for s in _ABORT_STAGE_ORDER if s in seen]
        extra = sorted(s for s in seen if s not in _ABORT_STAGE_ORDER)
        flushed = ordered + extra
        if flushed:
            print(f"  Stages that flushed client {cid} state: {len(flushed)}")
            for stage in flushed:
                _dt, hms = seen[stage]
                print(ref.green(f"    {hms}  {stage}"))
        else:
            print(ref.red(f"  No worker stage logged an abort flush for client {cid}"))
            overall_ok = False
        print()

    print(ref.green("Result: client-disconnect cleanup propagated ✓") if overall_ok
          else ref.red("Result: client-disconnect cleanup INCOMPLETE ✗"))
    print(BAR)
    return overall_ok


# --------------------------------------------------------------------------- #
# summary footer
# --------------------------------------------------------------------------- #
def print_summary(results, timings, sampler, wall, timed_out, num_clients,
                  inputs_label, aborted_clients=frozenset()):
    print()
    print(BAR)
    print("FULL PIPELINE TEST SUMMARY")
    print(BAR)
    print(f"Inputs: {inputs_label}   Clients: {num_clients}")
    if aborted_clients:
        print(ref.yellow(
            "Disconnected mid-query (monkey, abort expected, excluded from "
            f"result validation): {', '.join(f'client_{c}' for c in sorted(aborted_clients))}"))
    print()
    print(f"{'Query':<8}{'Result':<10}{'Pipeline time':<16}")
    for q in QUERIES:
        passed = results.get(q)
        cell = f"{'✓ PASS' if passed else '✗ FAIL':<10}"
        cell = ref.green(cell) if passed else ref.red(cell)
        t = timings.get(q)
        t_str = f"{t:.1f}s" if t is not None else "—"
        print(f"{q.upper():<8}{cell}{t_str:<16}")
    print()
    print(f"Total wall time : {wall:.1f}s")
    if timed_out:
        print("WARNING: client wait timed out before all clients finished")
    print(f"Containers      : {len(sampler.containers)}")
    print(f"Peak CPU (total): {sampler.peak_total_cpu:.1f}%")
    print(f"Peak RAM (total): {sampler.peak_total_mem:.0f} MiB "
          f"({sampler.peak_total_mem / 1024:.2f} GiB)")

    def top(d, n=3):
        return sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]

    if sampler.peak_cpu_by_container:
        print("Top CPU spikes  : " + ", ".join(
            f"{name.replace(TEST_PROJECT + '-', '')}={v:.0f}%"
            for name, v in top(sampler.peak_cpu_by_container)))
    if sampler.peak_mem_by_container:
        print("Top RAM spikes  : " + ", ".join(
            f"{name.replace(TEST_PROJECT + '-', '')}={v:.0f}MiB"
            for name, v in top(sampler.peak_mem_by_container)))
    print(BAR)
    overall = all(results.values()) and not timed_out
    if overall:
        print(ref.green("✓✓✓ FULL PIPELINE TEST PASSED ✓✓✓"))
    else:
        print(ref.red("✗✗✗ FULL PIPELINE TEST FAILED ✗✗✗"))
    print(BAR)
    return overall


# --------------------------------------------------------------------------- #
# persistent log archive
# --------------------------------------------------------------------------- #
LOG_ARCHIVE_DIR = ROOT / "data" / "logs"
# Keep at most this many past runs (0 = unlimited) and cap the total size of the
# archive (0 = unlimited). Oldest runs are deleted first; the current run is
# never deleted.
LOG_KEEP = int(os.environ.get("TEST_LOG_KEEP", "5"))
LOG_MAX_MB = int(os.environ.get("TEST_LOG_MAX_MB", "1024"))


def _archived_logs():
    return sorted(LOG_ARCHIVE_DIR.glob("test-*.log"), key=lambda p: p.stat().st_mtime)


def prune_log_archive():
    """Bound the archive: keep newest LOG_KEEP runs and stay under LOG_MAX_MB."""
    logs = _archived_logs()
    if LOG_KEEP > 0:
        for old in logs[:-LOG_KEEP]:
            old.unlink(missing_ok=True)
    if LOG_MAX_MB > 0:
        logs = _archived_logs()
        cap = LOG_MAX_MB * 1024 * 1024
        total = sum(p.stat().st_size for p in logs)
        for old in logs[:-1]:  # never delete the most recent run
            if total <= cap:
                break
            total -= old.stat().st_size
            old.unlink(missing_ok=True)


def point_latest_at(log_path):
    """Maintain data/logs/latest.log -> this run, so the last run's full log is
    always at a stable path regardless of its timestamped name."""
    latest = LOG_ARCHIVE_DIR / "latest.log"
    try:
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(Path(log_path).name)
    except OSError:
        pass


_log_finalized = False


def finalize_log_archive(log_path):
    """Prune, refresh the latest.log pointer, and print where the full log is.
    Safe to call more than once (e.g. on normal exit and on interrupt)."""
    global _log_finalized
    if _log_finalized:
        return
    _log_finalized = True
    prune_log_archive()
    point_latest_at(log_path)
    try:
        size_mb = os.path.getsize(log_path) / (1024 * 1024)
        print()
        print(ref.green(f"Full run log saved: {log_path} ({size_mb:.1f} MiB)"))
        print(f"   Stable pointer: {LOG_ARCHIVE_DIR / 'latest.log'}")
        print(f"   Keeping last {LOG_KEEP} run(s) under {LOG_MAX_MB} MiB in {LOG_ARCHIVE_DIR}")
    except OSError:
        pass


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main():
    try:
        client_inputs = load_client_inputs()
    except Exception as e:  # noqa: BLE001
        print(f"ERROR loading client inputs from {COMPOSE_FILE}: {e}", file=sys.stderr)
        return 2
    inputs_label = dataset_summary(client_inputs)

    LOG_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    prune_log_archive()  # drop older runs before this one starts
    log_path = str(LOG_ARCHIVE_DIR / f"test-{datetime.now():%Y%m%d-%H%M%S}.log")
    open(log_path, "w").close()
    point_latest_at(log_path)  # discoverable from the first line, even if interrupted
    print(f"test_log_file={log_path}  (also at {LOG_ARCHIVE_DIR / 'latest.log'})")

    cleanup_all()
    if not ensure_reference(client_inputs):
        return 2

    log_proc = None
    sampler = MetricsSampler(TEST_PROJECT, METRICS_INTERVAL)
    watcher = None
    timed_out = False
    interrupted = False
    start = time.monotonic()

    try:
        print(f"Building and starting full test stack ({inputs_label})...")
        if run(compose("up", "--build", "--remove-orphans", "--detach")).returncode != 0:
            print("ERROR: docker compose up failed", file=sys.stderr)
            return 2

        services = subprocess.run(
            compose("config", "--services"), capture_output=True, text=True).stdout.split()
        clients = [s for s in services if s.startswith("client_")]
        if not clients:
            print("ERROR: no client services found", file=sys.stderr)
            return 2
        num_clients = len(clients)

        sampler.start()
        watcher = TimingWatcher(log_path, num_clients, start)
        watcher.start()

        # stream logs -> pretty_logs (tee raw lines for the watcher/validation)
        log_cmd = (
            f'docker compose -p {TEST_PROJECT} -f {COMPOSE_FILE} '
            f'logs --follow --timestamps --no-color | '
            f'{sys.executable} {PRETTY_LOGS} --color {LOG_COLOR} --tee-file {log_path}'
        )
        log_proc = subprocess.Popen(log_cmd, shell=True, start_new_session=True)

        print(f"Waiting for {num_clients} client(s) to finish (timeout {WAIT_TIMEOUT})...")
        try:
            run(compose("wait", *clients), timeout=_seconds(WAIT_TIMEOUT),
                stdout=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            timed_out = True
            print("WARNING: timed out waiting for clients", file=sys.stderr)
        except KeyboardInterrupt:
            interrupted = True
            print("\nInterrupted — stopping the run early; the captured log is "
                  "preserved.", file=sys.stderr)

        # give the gateway/clients a moment to flush final logs + output files
        time.sleep(3)
    finally:
        sampler.stop()
        if watcher is not None:
            watcher.stop()
            watcher.join(timeout=5)
        if sampler.is_alive():
            sampler.join(timeout=5)
        if log_proc is not None:
            _kill_pipeline(log_proc)

    wall = time.monotonic() - start
    print()
    aborted_clients = aborted_clients_from_log(log_path)
    if interrupted:
        print_monkey_summary(log_path)
        print_abort_summary(log_path, aborted_clients)
        if not KEEP_CONTAINERS:
            teardown()
        finalize_log_archive(log_path)
        return 130

    results = validate_all(client_inputs, aborted_clients)
    print_election_summary(log_path)
    print_monkey_summary(log_path)
    abort_ok = print_abort_summary(log_path, aborted_clients)
    overall = print_summary(
        results, (watcher.done_at if watcher else {}), sampler, wall,
        timed_out, num_clients, inputs_label, aborted_clients)
    # When the monkey disconnected a client, the run only passes if cleanup
    # actually propagated. abort_ok is None when no client was disconnected.
    if abort_ok is False:
        overall = False

    if KEEP_CONTAINERS:
        print(f"KEEP_CONTAINERS set — leaving stack up. Tear down with:\n"
              f"  docker compose -p {TEST_PROJECT} -f {COMPOSE_FILE} down --volumes --remove-orphans")
    else:
        teardown()

    finalize_log_archive(log_path)
    return 0 if overall else 1


def _seconds(value):
    value = value.strip().lower()
    if value.endswith("s"):
        value = value[:-1]
    return float(value)


def _kill_pipeline(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


if __name__ == "__main__":
    sys.exit(main())
