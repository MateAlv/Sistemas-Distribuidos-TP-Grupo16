"""WorkerState adapter for the file splitter.

Wraps the splitter's mutable business state behind the snapshot/restore/apply_change
contract the durable-state engine expects, and is the single source of truth for
both the live output and the WAL replay.

Output emission vs state mutation
---------------------------------
The durable handler requires the business function to be side-effect free: it
returns the logical outputs without touching worker state, and the handler then
applies the recorded change (also on replay). For the splitter the outputs of a
chunk depend on accumulated buffer state (partial lines in the LineSplitter, lines
buffered toward the next batch), so they cannot be computed from the chunk payload
alone. ``simulate`` therefore runs the exact same accumulation as ``apply_change``
on a *copy* of the affected file, collecting the emitted outputs without mutating
real state; ``apply_change`` re-runs it on the real state to advance the buffers.
Sharing ``_run_*`` between both keeps the emitted outputs and the applied state in
lockstep.

Change types (all JSON-safe)
-----------------------------
  "chunk"      raw chunk payload (base64); replays parsing, header detection and
               batch accumulation so batch_id / counters stay in sync.
  "file_eof"   drains the LineSplitter, flushes the last partial batch, removes
               the file from the tracked set.
  "client_eof" applies the same finish logic to every remaining open file.

Note: this adapter has no EofCoordinator and no per-client EOF counter. The
splitter is a pure chunked-stream parser; downstream workers handle the
multi-sender EOF coordination.
"""

from __future__ import annotations

import base64
import dataclasses
from dataclasses import dataclass, field

from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
)
from common.message_protocol.internal import (
    ControlMessage,
    ControlMessageSerializer,
    InternalProtocol,
    LineBatch,
    LineBatchSerializer,
    MessageType,
)
from workers.common.line_splitter import LineSplitter, parse_csv_line


@dataclass(frozen=True)
class FileKey:
    client_id: int
    rel_path: str


@dataclass
class FileState:
    splitter: LineSplitter
    file_type: int | None = None
    header: tuple[str, ...] | None = None
    batch_lines: list[bytes] = field(default_factory=list)
    batch_first_line_number: int = 0
    batch_payload_bytes: int = 0
    batch_id: int = 0
    lines_seen: int = 0
    data_lines_emitted: int = 0
    batches_emitted: int = 0
    chunks_received: int = 0
    bytes_received: int = 0

# Logical output edges. The worker maps these to concrete publisher-registry keys
# (the sharded line-batch exchange and the accounts queue).
LINE_BATCH_EDGE = "line_batch"
ACCOUNTS_EDGE = "accounts"

# Fixed per-packet overhead must stay in sync with _batch_packet_size() in
# file_splitter.py — any divergence causes flush decisions on replay to differ
# from live, corrupting batch_id counters.
_PACKET_FIXED = InternalProtocol.HEADER_SIZE + LineBatchSerializer.FIXED_HEADER_SIZE
_ADDRESSED_PACKET_FIXED = (
    InternalProtocol.ADDRESSED_HEADER_SIZE + LineBatchSerializer.FIXED_HEADER_SIZE
)

# One emitted output: (edge, msg_type, payload). The payload is a plain serialized
# LineBatch (DATA) or ControlMessage (EOF); the handler's SenderSequencer rebuilds
# it as an addressed packet on the way out.
EmittedOutput = tuple[str, int, bytes]


class FileSplitterState:
    def __init__(
        self,
        max_line_bytes: int,
        max_batch_bytes: int,
        splitter_id: int,
        accounts_enabled: bool = True,
        # accounts_enabled mirrors `self._config.accounts_output_queue is not None`
        # in the live worker: when False, account-file lines are dropped after
        # incrementing lines_seen, exactly as _handle_line does.
    ) -> None:
        self._max_line_bytes = max_line_bytes
        self._max_batch_bytes = max_batch_bytes
        self._splitter_id = splitter_id
        self._accounts_enabled = accounts_enabled
        self._line_batch_serializer = LineBatchSerializer()
        self._control_serializer = ControlMessageSerializer()
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

    def expected_offset(self, key: FileKey) -> int:
        """Next byte offset the file expects. 0 when the file is unknown. Drives
        the worker's in-order chunk dedup guard."""
        state = self._files.get(key)
        return state.splitter.expected_offset if state is not None else 0

    def next_ordinal(self, key: FileKey) -> int:
        """Dense per-file chunk ordinal that the next NEW chunk will get (= number
        of chunks already applied to this file)."""
        state = self._files.get(key)
        return state.chunks_received if state is not None else 0

    # ---------- output simulation (read-only) ----------

    def simulate(self, change: dict) -> list[EmittedOutput]:
        """Compute the outputs ``apply_change`` would emit, without mutating real
        state. Runs the shared accumulation on a copy of the affected file(s)."""
        out: list[EmittedOutput] = []
        kind = change["type"]
        if kind == "chunk":
            key = FileKey(client_id=change["client_id"], rel_path=change["rel_path"])
            state = self._copy_or_new(key)
            self._run_chunk(key, state, change, dict(self._accounts_batches_by_client), out)
        elif kind == "file_eof":
            key = FileKey(client_id=change["client_id"], rel_path=change["rel_path"])
            state = self._files.get(key)
            if state is not None:
                self._run_finish(
                    key, _copy_file_state(state), dict(self._accounts_batches_by_client), out
                )
        elif kind == "client_eof":
            accounts_counts = dict(self._accounts_batches_by_client)
            for key in [k for k in self._files if k.client_id == change["client_id"]]:
                self._run_finish(key, _copy_file_state(self._files[key]), accounts_counts, out)
        else:
            raise ValueError(f"unknown change type: {kind}")
        return out

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
            key = FileKey(client_id=change["client_id"], rel_path=change["rel_path"])
            state = self._state_for(key)
            self._run_chunk(key, state, change, self._accounts_batches_by_client, None)
            self._chunks_received += 1
        elif kind == "file_eof":
            self._finish_and_remove(
                FileKey(client_id=change["client_id"], rel_path=change["rel_path"])
            )
            self._eofs_received += 1
        elif kind == "client_eof":
            for key in [k for k in self._files if k.client_id == change["client_id"]]:
                self._finish_and_remove(key)
            self._eofs_received += 1
        else:
            raise ValueError(f"unknown change type: {kind}")

    # ---------- shared accumulation ----------
    #
    # Each _run_* mutates the FileState it is given and, when `out` is not None,
    # appends the emitted plain outputs. apply_change passes the real state and
    # out=None (mutation only); simulate passes a copy and a collector (outputs
    # only). Both must run identical logic so emitted outputs match applied state.

    def _run_chunk(
        self, key: FileKey, state: FileState, change: dict, accounts_counts: dict, out
    ) -> None:
        if state.file_type is None:
            # file_type is set once from the first chunk; subsequent chunks carry it
            # redundantly but we ignore them to match live worker behavior.
            state.file_type = change["file_type"]
        payload = base64.b64decode(change["payload_b64"])
        for line in state.splitter.push(change["offset"], payload):
            self._run_line(key, state, line, accounts_counts, out)
        state.bytes_received += len(payload)
        state.chunks_received += 1

    def _run_line(
        self, key: FileKey, state: FileState, line: bytes, accounts_counts: dict, out
    ) -> None:
        state.lines_seen += 1
        if state.file_type == FILE_TYPE_ACCOUNTS and not self._accounts_enabled:
            return
        if state.header is None:
            # LineSplitter splits on \n only, so CRLF files leave a trailing \r
            # that must be stripped before CSV parsing.
            clean = line[:-1] if line.endswith(b"\r") else line
            state.header = tuple(parse_csv_line(clean))
            return
        self._run_accumulate(key, state, line, accounts_counts, out)

    def _run_accumulate(
        self, key: FileKey, state: FileState, line: bytes, accounts_counts: dict, out
    ) -> None:
        # 4-byte length prefix per string item, matching LineBatchSerializer wire format.
        line_size = 4 + len(line)
        if state.batch_lines and self._packet_size(key, state, line_size) > self._max_batch_bytes:
            self._run_flush(key, state, accounts_counts, out)
        if not state.batch_lines:
            state.batch_first_line_number = state.lines_seen
            state.batch_payload_bytes = 0
        state.batch_lines.append(line)
        state.batch_payload_bytes += line_size

    def _run_flush(
        self, key: FileKey, state: FileState, accounts_counts: dict, out
    ) -> None:
        if not state.batch_lines:
            return
        if out is not None:
            batch = LineBatch(
                file_type=state.file_type,
                rel_path=key.rel_path,
                batch_id=state.batch_id,
                first_line_number=state.batch_first_line_number,
                header=state.header,
                lines=tuple(state.batch_lines),
            )
            edge = ACCOUNTS_EDGE if state.file_type == FILE_TYPE_ACCOUNTS else LINE_BATCH_EDGE
            out.append((edge, MessageType.DATA, self._line_batch_serializer.serialize(batch)))
        state.data_lines_emitted += len(state.batch_lines)
        state.batches_emitted += 1
        if state.file_type == FILE_TYPE_ACCOUNTS:
            accounts_counts[key.client_id] = accounts_counts.get(key.client_id, 0) + 1
        state.batch_id += 1
        state.batch_lines = []
        state.batch_first_line_number = 0
        state.batch_payload_bytes = 0

    def _run_finish(
        self, key: FileKey, state: FileState, accounts_counts: dict, out
    ) -> None:
        for line in state.splitter.finish():
            self._run_line(key, state, line, accounts_counts, out)
        self._run_flush(key, state, accounts_counts, out)
        if out is not None:
            if state.file_type == FILE_TYPE_TRANSACTIONS:
                out.append(
                    (LINE_BATCH_EDGE, MessageType.EOF, self._eof_payload(state.data_lines_emitted))
                )
            elif state.file_type == FILE_TYPE_ACCOUNTS and self._accounts_enabled:
                out.append(
                    (
                        ACCOUNTS_EDGE,
                        MessageType.EOF,
                        self._eof_payload(accounts_counts.get(key.client_id, 0)),
                    )
                )
        if state.file_type == FILE_TYPE_ACCOUNTS and self._accounts_enabled:
            # Mirrors _send_accounts_eof() in the live worker, which pops the
            # counter when emitting the accounts EOF output.
            accounts_counts.pop(key.client_id, None)

    def _finish_and_remove(self, key: FileKey) -> None:
        state = self._files.get(key)
        if state is None:
            return
        self._run_finish(key, state, self._accounts_batches_by_client, None)
        del self._files[key]

    def _eof_payload(self, expected_total: int) -> bytes:
        return self._control_serializer.serialize(
            ControlMessage(
                sender_id=self._splitter_id,
                expected_total=expected_total,
                processed_count=0,
            )
        )

    def _packet_size(self, key: FileKey, state: FileState, extra: int = 0) -> int:
        # Reproduces _batch_packet_size() from file_splitter.py exactly.
        # Divergence here would cause flush triggers to differ between live and
        # replay, producing mismatched batch_id sequences and wrong expected_total.
        rel_path_bytes = len(key.rel_path.encode("utf-8"))
        header_bytes = sum(4 + len(col.encode("utf-8")) for col in (state.header or ()))
        fixed_size = (
            _ADDRESSED_PACKET_FIXED
            if state.file_type == FILE_TYPE_ACCOUNTS
            else _PACKET_FIXED
        )
        return fixed_size + rel_path_bytes + header_bytes + state.batch_payload_bytes + extra

    def _state_for(self, key: FileKey) -> FileState:
        state = self._files.get(key)
        if state is None:
            state = FileState(splitter=LineSplitter(self._max_line_bytes))
            self._files[key] = state
        return state

    def _copy_or_new(self, key: FileKey) -> FileState:
        state = self._files.get(key)
        if state is None:
            return FileState(splitter=LineSplitter(self._max_line_bytes))
        return _copy_file_state(state)


def _copy_file_state(state: FileState) -> FileState:
    # Cheap copy for read-only simulation: the only field the accumulation mutates
    # in place is batch_lines (appended to / replaced), so a shallow list copy is
    # enough — pending and header are immutable and safe to share. Avoids
    # copy.deepcopy, which is markedly slower on the splitter's hot path.
    return FileState(
        splitter=LineSplitter(
            max_line_bytes=state.splitter.max_line_bytes,
            expected_offset=state.splitter.expected_offset,
            pending=state.splitter.pending,
        ),
        file_type=state.file_type,
        header=state.header,
        batch_lines=list(state.batch_lines),
        batch_first_line_number=state.batch_first_line_number,
        batch_payload_bytes=state.batch_payload_bytes,
        batch_id=state.batch_id,
        lines_seen=state.lines_seen,
        data_lines_emitted=state.data_lines_emitted,
        batches_emitted=state.batches_emitted,
        chunks_received=state.chunks_received,
        bytes_received=state.bytes_received,
    )


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
        # restore the tuple invariant that _run_line and _packet_size expect.
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
