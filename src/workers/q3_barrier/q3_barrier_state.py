"""WorkerState adapter for the q3_barrier worker.

Wraps the barrier's per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

This worker joins two independent streams (averages + candidates) per client and
emits the filtered candidates once both streams have signalled EOF (the barrier).
No EofCoordinator is used.

Durable disk log (named file)
-----------------------------
Candidate batches are appended to a per-client named file under STATE_DIR
(``q3cand_{node_id}_{client_id}.log``). The file survives a crash; the snapshot
records each open client's byte length, and restore() truncates the file back to
that length so a WAL replay re-appends exactly the post-checkpoint batches (no
double-append). The append-then-WAL ordering is made consistent by this
truncate-to-snapshot-offset + replay, so the file always matches the WAL.

Change types
------------
  "avg_data"        one average DATA (payment_format -> average; idempotent set).
  "avg_eof"         averages stream EOF.
  "candidate_data"  one candidate batch; raw payload (base64) re-appended on replay.
  "candidate_eof"   candidates stream EOF (carries expected_total).
  "close"           both streams done + emitted; drop state and delete the file.
  "compound"        apply several of the above atomically (EOF that closes the
                    barrier bundles eof + close).

State accessors (read before close_change, at emit time)
--------------------------------------------------------
  averages(client_id)   -> dict[str, float]
  disk_log(client_id)   -> _DiskLog | None   (iterate raw batches from disk)
  is_ready(client_id)   -> bool  (both stream EOFs received)
"""

from __future__ import annotations

import base64
import glob
import os

_RECORD_LEN_SIZE = 4


class _DiskLog:
    """Append-only log backed by a named file under STATE_DIR.

    Durable across crashes: the file persists, the snapshot records ``byte_count``,
    and restore() truncates back to it so WAL replay re-appends the rest."""

    def __init__(self, path: str) -> None:
        self._path = path
        # "a+b" creates if missing and appends; reopened verbatim on restore.
        self._file = open(path, "a+b")
        self._file.seek(0, os.SEEK_END)
        self.byte_count: int = self._file.tell()

    def append(self, payload: bytes) -> None:
        n = len(payload)
        self._file.write(n.to_bytes(_RECORD_LEN_SIZE, "big"))
        self._file.write(payload)
        self._file.flush()
        self.byte_count += _RECORD_LEN_SIZE + n

    def truncate_to(self, byte_count: int) -> None:
        """Drop everything past ``byte_count`` (post-snapshot appends); replay
        re-adds exactly those records."""
        self._file.truncate(byte_count)
        self._file.seek(0, os.SEEK_END)
        self.byte_count = self._file.tell()

    def iter_raw_batches(self):
        """Sequential iterator; yields raw batch payloads in append order. O(1) RAM."""
        self._file.seek(0)
        while True:
            header = self._file.read(_RECORD_LEN_SIZE)
            if not header or len(header) < _RECORD_LEN_SIZE:
                return
            n = int.from_bytes(header, "big")
            yield self._file.read(n)

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:
            pass

    def delete(self) -> None:
        self.close()
        try:
            os.remove(self._path)
        except OSError:
            pass


class Q3BarrierState:
    def __init__(self, state_dir: str, node_id: str) -> None:
        self._state_dir = state_dir or "."
        self._node_id = node_id
        self._averages_by_client: dict[int, dict[str, float]] = {}
        self._avg_eof_by_client: set[int] = set()
        self._candidates_eof_by_client: set[int] = set()
        self._expected_total_by_client: dict[int, int] = {}
        self._closed_by_client: set[int] = set()
        # Disk log content lives in the named files; the snapshot only records the
        # byte length per open client (see _DiskLog).
        self._disk_logs: dict[int, _DiskLog] = {}

    # ---------- change constructors ----------

    @staticmethod
    def avg_data_change(client_id: int, payment_format: str, average: float) -> dict:
        return {
            "type": "avg_data",
            "client_id": client_id,
            "payment_format": payment_format,
            "average": average,
        }

    @staticmethod
    def avg_eof_change(client_id: int) -> dict:
        return {"type": "avg_eof", "client_id": client_id}

    @staticmethod
    def candidate_data_change(client_id: int, payload: bytes) -> dict:
        return {
            "type": "candidate_data",
            "client_id": client_id,
            "payload_b64": base64.b64encode(payload).decode("ascii"),
        }

    @staticmethod
    def candidate_eof_change(client_id: int, expected_total: int) -> dict:
        return {
            "type": "candidate_eof",
            "client_id": client_id,
            "expected_total": expected_total,
        }

    @staticmethod
    def close_change(client_id: int) -> dict:
        return {"type": "close", "client_id": client_id}

    @staticmethod
    def abort_change(client_id: int) -> dict:
        return Q3BarrierState.close_change(client_id)

    @staticmethod
    def compound_change(*changes: dict) -> dict:
        return {"type": "compound", "changes": list(changes)}

    # ---------- read accessors ----------

    def averages(self, client_id: int) -> dict[str, float]:
        return self._averages_by_client.get(client_id, {})

    def disk_log(self, client_id: int) -> _DiskLog | None:
        return self._disk_logs.get(client_id)

    def has_avg_eof(self, client_id: int) -> bool:
        return client_id in self._avg_eof_by_client

    def has_candidate_eof(self, client_id: int) -> bool:
        return client_id in self._candidates_eof_by_client

    def is_ready(self, client_id: int) -> bool:
        return (
            client_id in self._avg_eof_by_client
            and client_id in self._candidates_eof_by_client
        )

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        return {
            "averages_by_client": {
                cid: dict(avgs) for cid, avgs in self._averages_by_client.items()
            },
            "avg_eof_by_client": set(self._avg_eof_by_client),
            "candidates_eof_by_client": set(self._candidates_eof_by_client),
            "expected_total_by_client": dict(self._expected_total_by_client),
            "closed_by_client": set(self._closed_by_client),
            # Per open client: the durable file's length at this checkpoint.
            "disk_log_bytes": {
                cid: log.byte_count for cid, log in self._disk_logs.items()
            },
        }

    def restore(self, data: dict) -> None:
        for log in self._disk_logs.values():
            log.close()
        self._disk_logs = {}

        if not data:
            self._averages_by_client = {}
            self._avg_eof_by_client = set()
            self._candidates_eof_by_client = set()
            self._expected_total_by_client = {}
            self._closed_by_client = set()
            return

        self._averages_by_client = {
            cid: dict(avgs) for cid, avgs in data["averages_by_client"].items()
        }
        self._avg_eof_by_client = set(data["avg_eof_by_client"])
        self._candidates_eof_by_client = set(data["candidates_eof_by_client"])
        self._expected_total_by_client = dict(data["expected_total_by_client"])
        self._closed_by_client = set(data["closed_by_client"])
        # Reopen each open client's file and truncate to the checkpoint length; the
        # WAL replay that follows re-appends the post-checkpoint candidate batches.
        for cid, byte_count in data.get("disk_log_bytes", {}).items():
            log = self._disk_log_for(cid)
            log.truncate_to(byte_count)

    def discard_disk_logs(self) -> None:
        """Delete this node's candidate files. Call only before a from-scratch
        recovery (no snapshot): with no checkpoint to truncate to, a file left by a
        previous life would be re-appended by REPLAY_ALL and double the candidates.
        Never call when a snapshot exists — the file holds the snapshotted bytes."""
        for log in self._disk_logs.values():
            log.close()
        self._disk_logs = {}
        pattern = os.path.join(self._state_dir, f"q3cand_{self._node_id}_*.log")
        for path in glob.glob(pattern):
            try:
                os.remove(path)
            except OSError:
                pass

    def apply_change(self, change: dict) -> None:
        kind = change["type"]
        if kind == "compound":            # check before reading client_id
            for sub in change["changes"]:
                self.apply_change(sub)
            return
        client_id = change["client_id"]
        if kind == "avg_data":
            self._apply_avg_data(client_id, change)
        elif kind == "avg_eof":
            self._apply_avg_eof(client_id)
        elif kind == "candidate_data":
            self._apply_candidate_data(client_id, change)
        elif kind == "candidate_eof":
            self._apply_candidate_eof(client_id, change)
        elif kind == "close":
            self._apply_close(client_id)
        else:
            raise ValueError(f"unknown change type: {kind}")

    def _apply_avg_data(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        avgs = self._averages_by_client.setdefault(client_id, {})
        avgs[change["payment_format"]] = change["average"]

    def _apply_avg_eof(self, client_id: int) -> None:
        if client_id in self._closed_by_client:
            return
        self._avg_eof_by_client.add(client_id)

    def _apply_candidate_data(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        payload = base64.b64decode(change["payload_b64"])
        self._disk_log_for(client_id).append(payload)

    def _apply_candidate_eof(self, client_id: int, change: dict) -> None:
        if client_id in self._closed_by_client:
            return
        self._candidates_eof_by_client.add(client_id)
        self._expected_total_by_client[client_id] = change["expected_total"]

    def _apply_close(self, client_id: int) -> None:
        log = self._disk_logs.pop(client_id, None)
        if log is not None:
            log.delete()
        self._averages_by_client.pop(client_id, None)
        self._avg_eof_by_client.discard(client_id)
        self._candidates_eof_by_client.discard(client_id)
        self._expected_total_by_client.pop(client_id, None)
        self._closed_by_client.add(client_id)

    def _disk_log_for(self, client_id: int) -> _DiskLog:
        log = self._disk_logs.get(client_id)
        if log is None:
            path = os.path.join(
                self._state_dir, f"q3cand_{self._node_id}_{client_id}.log"
            )
            log = _DiskLog(path)
            self._disk_logs[client_id] = log
        return log
