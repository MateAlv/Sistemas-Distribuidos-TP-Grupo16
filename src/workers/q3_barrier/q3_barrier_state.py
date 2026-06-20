"""WorkerState adapter for the q3_barrier worker.

Wraps the barrier's per-client state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

This worker receives data from two independent streams (averages + candidates)
and emits filtered candidates once both streams have fully arrived. No
EofCoordinator is used.

STATE LIMITATION — disk log
-----------------------------
Candidate batches are stored in a _DiskLog backed by tempfile.TemporaryFile().
TemporaryFile is anonymous (no filesystem path on Linux after creation) so its
content cannot be included in snapshot(). Only the "light" state is snapshotted:
averages, EOF flags, and expected_total counters.

On crash recovery the disk log is rebuilt entirely from WAL replay: every
"candidate_data" change that arrived AFTER the last snapshot checkpoint is
re-applied by apply_change, which re-appends the raw batch to a fresh disk log.
Batches that arrived BEFORE the last snapshot checkpoint are lost and cannot be
recovered without replacing TemporaryFile with a named file under STATE_DIR.

Mitigation options (not implemented):
  - Set snapshot_interval = ∞ so the WAL accumulates all messages; a full
    replay always rebuilds the disk log completely.
  - Replace _DiskLog with a named file in STATE_DIR and include its path in
    snapshot() so restore() can re-open it.

Change types
------------
  "avg_data"
      One average DATA arrived. Carries payment_format + average value as JSON-
      safe primitives (no base64 needed).
  "avg_eof"
      Averages stream EOF arrived. Sets avg_expected_total.
  "candidate_data"
      One candidate batch arrived. Carries the raw payload (base64) so
      apply_change can re-append it to the disk log on WAL replay.
  "candidate_eof"
      Candidates stream EOF arrived. Sets candidates_expected_total.
  "close"
      Client was fully emitted and cleaned up. Closes the disk log fd and
      marks the client closed.

Caller protocol (one change dict per handle() call to PersistentStateHandler)
------------------------------------------------------------------------------
  Averages DATA message
    → avg_data_change(client_id, payment_format, avg_value)

  Averages EOF message
    → avg_eof_change(client_id, expected_total)
    If both streams complete after applying: emit + → close_change(client_id)

  Candidates DATA message
    → candidate_data_change(client_id, payload)
    (Batches are buffered in the disk log; no emit until both streams done.)

  Candidates EOF message
    → candidate_eof_change(client_id, expected_total)
    If both streams complete after applying:
      Stream disk log → filter by average → emit to outbox.
      → close_change(client_id)

State accessors (read before close_change)
------------------------------------------
  averages_for(client_id)   → dict[str, float]  payment_format → average
  disk_log_for(client_id)   → _DiskLog          iterate batches from disk
  is_ready(client_id)       → bool  True when both stream EOFs received + counts match
"""

from __future__ import annotations

import base64
import tempfile

_RECORD_LEN_SIZE = 4


class _DiskLog:
    """Append-only log backed by a TemporaryFile; rebuilt from WAL on recovery."""

    def __init__(self) -> None:
        self._file = tempfile.TemporaryFile()
        self.batch_count: int = 0
        self.byte_count: int = 0

    def append(self, payload: bytes) -> None:
        n = len(payload)
        self._file.write(n.to_bytes(_RECORD_LEN_SIZE, "big"))
        self._file.write(payload)
        self.batch_count += 1
        self.byte_count += n

    def iter_raw_batches(self):
        """Sequential iterator; yields raw batch payloads in append order."""
        self._file.seek(0)
        while True:
            header = self._file.read(_RECORD_LEN_SIZE)
            if not header:
                return
            n = int.from_bytes(header, "big")
            yield self._file.read(n)

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:
            pass


class Q3BarrierState:
    def __init__(self) -> None:
        self._averages_by_client: dict[int, dict[str, float]] = {}
        self._avg_eof_by_client: set[int] = set()
        self._candidates_eof_by_client: set[int] = set()
        self._expected_total_by_client: dict[int, int] = {}
        self._closed_by_client: set[int] = set()
        # Disk logs are intentionally NOT in the snapshot — see module docstring.
        self._disk_logs: dict[int, _DiskLog] = {}

    @staticmethod
    def avg_data_change(client_id: int, payment_format: str, average: float) -> dict:
        # payment_format and average are JSON-safe primitives; no base64 needed.
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
        # payload is base64 because the WAL frames state_change dicts as JSON.
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

    def averages(self, client_id: int) -> dict[str, float]:
        return self._averages_by_client.get(client_id, {})

    def disk_log(self, client_id: int) -> _DiskLog | None:
        """Read-only access to disk log; used at emit time before close_change."""
        return self._disk_logs.get(client_id)

    def is_ready(self, client_id: int) -> bool:
        """Both streams have signalled EOF — emit can proceed."""
        return (
            client_id in self._avg_eof_by_client
            and client_id in self._candidates_eof_by_client
        )

    def is_closed(self, client_id: int) -> bool:
        return client_id in self._closed_by_client

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        # disk_log content is intentionally excluded — see module docstring.
        return {
            "averages_by_client": {
                cid: dict(avgs)
                for cid, avgs in self._averages_by_client.items()
            },
            "avg_eof_by_client": set(self._avg_eof_by_client),
            "candidates_eof_by_client": set(self._candidates_eof_by_client),
            "expected_total_by_client": dict(self._expected_total_by_client),
            "closed_by_client": set(self._closed_by_client),
        }

    def restore(self, data: dict) -> None:
        # Discard existing disk logs — WAL replay will rebuild them.
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
            cid: dict(avgs)
            for cid, avgs in data["averages_by_client"].items()
        }
        self._avg_eof_by_client = set(data["avg_eof_by_client"])
        self._candidates_eof_by_client = set(data["candidates_eof_by_client"])
        self._expected_total_by_client = dict(data["expected_total_by_client"])
        self._closed_by_client = set(data["closed_by_client"])

    def apply_change(self, change: dict) -> None:
        # Single mutation path — runs both live and during WAL replay.
        kind = change["type"]
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
        # Close and discard disk log before marking closed — idempotent.
        log = self._disk_logs.pop(client_id, None)
        if log is not None:
            log.close()
        self._averages_by_client.pop(client_id, None)
        self._avg_eof_by_client.discard(client_id)
        self._candidates_eof_by_client.discard(client_id)
        self._expected_total_by_client.pop(client_id, None)
        self._closed_by_client.add(client_id)

    def _disk_log_for(self, client_id: int) -> _DiskLog:
        log = self._disk_logs.get(client_id)
        if log is None:
            log = _DiskLog()
            self._disk_logs[client_id] = log
        return log
