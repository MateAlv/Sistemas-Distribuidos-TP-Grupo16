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
import tempfile
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
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


def validate_query_for_clients(query, client_inputs, output_dir="data/output"):
    print("=" * 60)
    print(f"{query.upper()} FLOW VALIDATION")
    print("=" * 60)
    all_ok = True
    for client_input in client_inputs:
        print(f"\n  Client {client_input.client_id}: {_input_label(client_input)}")
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


def validate_all(client_inputs):
    results = {}
    for q in QUERIES:
        results[q] = validate_query_for_clients(q, client_inputs)
    return results


# --------------------------------------------------------------------------- #
# summary footer
# --------------------------------------------------------------------------- #
def print_summary(results, timings, sampler, wall, timed_out, num_clients, inputs_label):
    print()
    print(BAR)
    print("FULL PIPELINE TEST SUMMARY")
    print(BAR)
    print(f"Inputs: {inputs_label}   Clients: {num_clients}")
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
# main
# --------------------------------------------------------------------------- #
def main():
    try:
        client_inputs = load_client_inputs()
    except Exception as e:  # noqa: BLE001
        print(f"ERROR loading client inputs from {COMPOSE_FILE}: {e}", file=sys.stderr)
        return 2
    inputs_label = dataset_summary(client_inputs)

    log_fd, log_path = tempfile.mkstemp(prefix=f"{TEST_PROJECT}.", suffix=".log")
    os.close(log_fd)
    print(f"test_log_file={log_path}")

    cleanup_all()
    if not ensure_reference(client_inputs):
        return 2

    log_proc = None
    sampler = MetricsSampler(TEST_PROJECT, METRICS_INTERVAL)
    watcher = None
    timed_out = False
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

        # give the gateway/clients a moment to flush final logs + output files
        time.sleep(3)
    finally:
        sampler.stop()
        if watcher is not None:
            watcher.stop()
            watcher.join(timeout=5)
        sampler.join(timeout=5)
        if log_proc is not None:
            _kill_pipeline(log_proc)

    wall = time.monotonic() - start
    print()
    results = validate_all(client_inputs)
    overall = print_summary(
        results, (watcher.done_at if watcher else {}), sampler, wall,
        timed_out, num_clients, inputs_label)

    if KEEP_CONTAINERS:
        print(f"KEEP_CONTAINERS set — leaving stack up. Tear down with:\n"
              f"  docker compose -p {TEST_PROJECT} -f {COMPOSE_FILE} down --volumes --remove-orphans")
    else:
        teardown()

    try:
        os.unlink(log_path)
    except OSError:
        pass
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
