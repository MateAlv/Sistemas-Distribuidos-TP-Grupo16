"""WorkerState adapter for the file splitter.

Wraps the splitter's mutable business state behind the snapshot/restore/apply_change
contract the durable-state engine expects.

Three kinds of change, all JSON-safe:
  - "chunk": carries the raw chunk payload (base64) so apply_change can replay the
    exact same parsing, header detection and batch-accumulation logic without
    re-sending any output.
  - "file_eof": drains the LineSplitter's pending bytes, flushes the last partial
    batch state, and removes the file from the tracked set.
  - "client_eof": iterates over all remaining files for the client and applies the
    same finish logic as "file_eof".

Why the chunk payload travels in the change dict (instead of the resulting state):
the batch-accumulation logic is tightly coupled to LineSplitter's internal cursor
(expected_offset, pending buffer). Storing the raw payload and re-running the
pipeline on replay is the only way to keep batch_id, data_lines_emitted and
batches_emitted perfectly in sync with what the live worker produced.
"""

from __future__ import annotations

import base64
import dataclasses

from common.message_protocol.external.types import FILE_TYPE_ACCOUNTS
from common.message_protocol.internal import InternalProtocol, LineBatchSerializer
from workers.common.line_splitter import LineSplitter, parse_csv_line
from workers.file_splitter.file_splitter import FileKey, FileState

# Fixed per-packet overhead: internal envelope header + LineBatch fixed header.
# Must stay in sync with _batch_packet_size() in file_splitter.py — any divergence
# causes flush decisions on replay to differ from live, corrupting batch_id counters.
_PACKET_FIXED = InternalProtocol.HEADER_SIZE + LineBatchSerializer.FIXED_HEADER_SIZE


class FileSplitterState:
    def __init__(
        self,
        max_line_bytes: int,
        max_batch_bytes: int,
        accounts_enabled: bool = True,
        # accounts_enabled mirrors `self._config.accounts_output_queue is not None`
        # in the live worker: when False, account-file lines are dropped after
        # incrementing lines_seen, exactly as _handle_line does.
    ) -> None:
        self._max_line_bytes = max_line_bytes
        self._max_batch_bytes = max_batch_bytes
        self._accounts_enabled = accounts_enabled
        self._files: dict[FileKey, FileState] = {}
        self._accounts_batches_by_client: dict[int, int] = {}
        self._chunks_received: int = 0
        self._eofs_received: int = 0

    @staticmethod
    def chunk_change(
        client_id: int, rel_path: str, file_type: int, offset: int, payload: bytes
    ) -> dict:
        # payload is base64 because the WAL frames state_change dicts as JSON.
        return {
            "type": "chunk",
            "client_id": client_id,
            "rel_path": rel_path,
            "file_type": file_type,
            "offset": offset,
            "payload_b64": base64.b64encode(payload).decode("ascii"),
        }

    @staticmethod
    def file_eof_change(client_id: int, rel_path: str) -> dict:
        return {"type": "file_eof", "client_id": client_id, "rel_path": rel_path}

    @staticmethod
    def client_eof_change(client_id: int) -> dict:
        return {"type": "client_eof", "client_id": client_id}

    def data_lines_emitted(self, key: FileKey) -> int:
        state = self._files.get(key)
        return state.data_lines_emitted if state is not None else 0

    def accounts_batches_for_client(self, client_id: int) -> int:
        return self._accounts_batches_by_client.get(client_id, 0)

    def file_keys_for_client(self, client_id: int) -> list[FileKey]:
        return [k for k in self._files if k.client_id == client_id]

    # ---------- WorkerState protocol ----------

    def snapshot(self) -> dict:
        # dataclasses.asdict() recurses into FileState → LineSplitter, converting
        # both to plain dicts. This avoids pickling class references so the snapshot
        # survives across code changes that rename or refactor the dataclasses.
        return {
            "files": {
                (k.client_id, k.rel_path): dataclasses.asdict(v)
                for k, v in self._files.items()
            },
            "accounts_batches_by_client": dict(self._accounts_batches_by_client),
            "chunks_received": self._chunks_received,
            "eofs_received": self._eofs_received,
        }

    def restore(self, data: dict) -> None:
        if not data:
            self._files = {}
            self._accounts_batches_by_client = {}
            self._chunks_received = 0
            self._eofs_received = 0
            return
        # Keys were stored as (client_id, rel_path) tuples because frozen dataclasses
        # can't be used as dict keys after going through asdict() serialization.
        self._files = {
            FileKey(client_id=k[0], rel_path=k[1]): _filestate_from_dict(v)
            for k, v in data["files"].items()
        }
        self._accounts_batches_by_client = dict(data["accounts_batches_by_client"])
        self._chunks_received = data["chunks_received"]
        self._eofs_received = data["eofs_received"]

    def apply_change(self, change: dict) -> None:
        # Single mutation path — runs both live and during WAL replay.
        kind = change["type"]
        if kind == "chunk":
            self._apply_chunk(change)
        elif kind == "file_eof":
            self._apply_file_eof(change["client_id"], change["rel_path"])
        elif kind == "client_eof":
            self._apply_client_eof(change["client_id"])
        else:
            raise ValueError(f"unknown change type: {kind}")

    # ---------- chunk processing ----------

    def _apply_chunk(self, change: dict) -> None:
        key = FileKey(client_id=change["client_id"], rel_path=change["rel_path"])
        state = self._state_for(key)
        if state.file_type is None:
            # file_type is set once from the first chunk; subsequent chunks carry it
            # redundantly but we ignore them to match live worker behavior.
            state.file_type = change["file_type"]
        payload = base64.b64decode(change["payload_b64"])
        for line in state.splitter.push(change["offset"], payload):
            self._process_line(key, state, line)
        state.bytes_received += len(payload)
        state.chunks_received += 1
        self._chunks_received += 1

    def _process_line(self, key: FileKey, state: FileState, line: bytes) -> None:
        state.lines_seen += 1
        if state.file_type == FILE_TYPE_ACCOUNTS and not self._accounts_enabled:
            return
        if state.header is None:
            # LineSplitter splits on \n only, so CRLF files leave a trailing \r
            # that must be stripped before CSV parsing.
            clean = line[:-1] if line.endswith(b"\r") else line
            state.header = tuple(parse_csv_line(clean))
            return
        self._accumulate_line(key, state, line)

    def _accumulate_line(self, key: FileKey, state: FileState, line: bytes) -> None:
        # 4-byte length prefix per string item, matching LineBatchSerializer wire format.
        line_size = 4 + len(line)
        if state.batch_lines and self._packet_size(key, state, line_size) > self._max_batch_bytes:
            self._flush_batch_state(key, state)
        if not state.batch_lines:
            state.batch_first_line_number = state.lines_seen
            state.batch_payload_bytes = 0
        state.batch_lines.append(line)
        state.batch_payload_bytes += line_size

    def _packet_size(self, key: FileKey, state: FileState, extra: int = 0) -> int:
        # Reproduces _batch_packet_size() from file_splitter.py exactly.
        # Divergence here would cause flush triggers to differ between live and
        # replay, producing mismatched batch_id sequences and wrong expected_total.
        rel_path_bytes = len(key.rel_path.encode("utf-8"))
        header_bytes = sum(4 + len(col.encode("utf-8")) for col in (state.header or ()))
        return _PACKET_FIXED + rel_path_bytes + header_bytes + state.batch_payload_bytes + extra

    def _flush_batch_state(self, key: FileKey, state: FileState) -> None:
        # Updates all batch counters as if a batch was sent; no actual I/O.
        if not state.batch_lines:
            return
        state.data_lines_emitted += len(state.batch_lines)
        state.batches_emitted += 1
        if state.file_type == FILE_TYPE_ACCOUNTS:
            self._accounts_batches_by_client[key.client_id] = (
                self._accounts_batches_by_client.get(key.client_id, 0) + 1
            )
        state.batch_id += 1
        state.batch_lines.clear()
        state.batch_first_line_number = 0
        state.batch_payload_bytes = 0

    # ---------- EOF processing ----------

    def _finish_file(self, key: FileKey) -> None:
        state = self._files.get(key)
        if state is None:
            return
        for line in state.splitter.finish():
            self._process_line(key, state, line)
        self._flush_batch_state(key, state)
        if state.file_type == FILE_TYPE_ACCOUNTS and self._accounts_enabled:
            # Mirrors _send_accounts_eof() in the live worker, which pops the
            # counter when emitting the accounts EOF output to the outbox.
            self._accounts_batches_by_client.pop(key.client_id, None)
        del self._files[key]

    def _apply_file_eof(self, client_id: int, rel_path: str) -> None:
        self._finish_file(FileKey(client_id=client_id, rel_path=rel_path))
        self._eofs_received += 1

    def _apply_client_eof(self, client_id: int) -> None:
        for key in [k for k in self._files if k.client_id == client_id]:
            self._finish_file(key)
        self._eofs_received += 1

    def _state_for(self, key: FileKey) -> FileState:
        state = self._files.get(key)
        if state is None:
            state = FileState(splitter=LineSplitter(self._max_line_bytes))
            self._files[key] = state
        return state


def _filestate_from_dict(d: dict) -> FileState:
    s = d["splitter"]
    return FileState(
        splitter=LineSplitter(
            max_line_bytes=s["max_line_bytes"],
            expected_offset=s["expected_offset"],
            # bytes() guards against future JSON round-trips that would yield list[int].
            pending=bytes(s["pending"]),
        ),
        file_type=d["file_type"],
        # asdict() converts tuple fields to lists in some serialization paths;
        # restore the tuple invariant that _process_line and _packet_size expect.
        header=tuple(d["header"]) if d["header"] is not None else None,
        batch_lines=[bytes(line) for line in d["batch_lines"]],
        batch_first_line_number=d["batch_first_line_number"],
        batch_payload_bytes=d["batch_payload_bytes"],
        batch_id=d["batch_id"],
        lines_seen=d["lines_seen"],
        data_lines_emitted=d["data_lines_emitted"],
        batches_emitted=d["batches_emitted"],
        chunks_received=d["chunks_received"],
        bytes_received=d["bytes_received"],
    )
