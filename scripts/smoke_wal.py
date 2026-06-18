#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.fault_tolerance.wal import InputApplied, InputDone, Wal, WALReplayer


def main() -> int:
    args = parse_args()
    state_dir, should_cleanup = prepare_state_dir(args.state_dir)
    reporter = Reporter(step=args.step, quiet=args.quiet)

    try:
        reporter.header("WAL smoke test")
        reporter.line(f"state_dir: {state_dir}")
        if should_cleanup:
            reporter.line("mode: temporary directory, cleaned up at the end")
        else:
            reporter.line("mode: user-provided directory, files kept for inspection")
        reporter.pause()

        run_smoke(state_dir, reporter)
        reporter.header("Smoke test completed")
        reporter.line("All WAL scenarios passed.")
        if not should_cleanup:
            reporter.line(f"Inspect files under: {state_dir}")
        return 0
    except Exception as exc:
        print(f"wal_smoke_failure | error={exc}", file=sys.stderr)
        if not should_cleanup:
            print(f"wal_smoke_state_kept | state_dir={state_dir}", file=sys.stderr)
        return 1
    finally:
        if should_cleanup:
            shutil.rmtree(state_dir, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a manual smoke test for the fault-tolerance WAL API."
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        help="Directory where wal.current/wal.previous are written. Defaults to a temp dir.",
    )
    parser.add_argument(
        "--step",
        action="store_true",
        help="Pause before each scenario so the WAL files can be inspected.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Print only compact pass/fail messages.",
    )
    return parser.parse_args()


def prepare_state_dir(path: Path | None) -> tuple[Path, bool]:
    if path is None:
        return Path(tempfile.mkdtemp(prefix="wal-smoke-")), True

    path.mkdir(parents=True, exist_ok=True)
    for wal_file in ("wal.current", "wal.previous"):
        (path / wal_file).unlink(missing_ok=True)
    return path, False


def run_smoke(state_dir: Path, reporter: "Reporter") -> None:
    applied = build_input_applied()
    done = InputDone(
        msg_id=applied.msg_id,
        client_id=applied.client_id,
        sender_id=applied.sender_id,
        seq=applied.seq,
    )

    check_applied_then_done(state_dir / "completed", applied, done, reporter)
    check_pending_outbox_recovery(state_dir / "pending", applied, reporter)
    check_checkpoint_replay(state_dir / "checkpoint", applied, done, reporter)
    check_rotation(state_dir / "rotation", done, reporter)


def build_input_applied() -> InputApplied:
    input_id = "filter_usd_0:client_1:42"
    return InputApplied(
        msg_id=input_id,
        client_id=1,
        sender_id=0,
        seq=42,
        state_change={"amount": 50, "currency": "USD"},
        outputs=[
            OutboxEntry(
                output_id=f"{input_id}#0",
                input_id=input_id,
                destination="sum_q2_1",
                body=b"serialized-message",
            )
        ],
    )


def check_applied_then_done(
    state_dir: Path,
    applied: InputApplied,
    done: InputDone,
    reporter: "Reporter",
) -> None:
    reporter.scenario("completed input")
    reporter.line("Write INPUT_APPLIED followed by INPUT_DONE.")
    reporter.line("Expected recovery: input is done and no outbox is pending.")
    reporter.pause()

    wal = Wal(state_dir)
    lsn_applied = wal.append(applied)
    lsn_done = wal.append(done)
    next_lsn = wal.current_record_number()
    wal.close()
    reporter.line(f"wrote INPUT_APPLIED at LSN {lsn_applied}")
    reporter.line(f"wrote INPUT_DONE at LSN {lsn_done}")
    reporter.line(f"next LSN is {next_lsn}")
    reporter.line(f"wal.current size: {file_size(state_dir / 'wal.current')} bytes")

    recovered = replay_records(state_dir)
    assert recovered == [applied, done], "append/replay roundtrip mismatch"
    reporter.line(f"replayed records: {record_names(recovered)}")

    result = replay_result(state_dir, checkpoint_lsn=-1)
    assert result.done_inputs == {done.msg_id: done}, "done input was not recovered"
    assert result.pending_outbox == {}, "completed input left pending outbox"
    assert result.outbox_to_republish() == [], "completed input should not republish"
    assert lsn_applied == 0, "first LSN should be zero"
    assert lsn_done > lsn_applied, "second LSN should be greater than first"
    assert next_lsn > lsn_done, "current record number should point after last record"
    reporter.ok("completed input leaves no pending outbox")


def check_pending_outbox_recovery(
    state_dir: Path,
    applied: InputApplied,
    reporter: "Reporter",
) -> None:
    reporter.scenario("crash after INPUT_APPLIED")
    reporter.line("Write only INPUT_APPLIED, as if the worker died before INPUT_DONE.")
    reporter.line("Expected recovery: state change is replayed and outbox is pending.")
    reporter.pause()

    wal = Wal(state_dir)
    lsn_applied = wal.append(applied)
    wal.close()
    reporter.line(f"wrote INPUT_APPLIED at LSN {lsn_applied}")
    reporter.line(f"wal.current size: {file_size(state_dir / 'wal.current')} bytes")

    result = replay_result(state_dir, checkpoint_lsn=-1)
    assert result.applied_inputs == {
        applied.msg_id: applied
    }, "applied input was not recovered"
    assert result.done_inputs == {}, "pending input should not be done"
    assert result.pending_outbox == {
        applied.msg_id: applied.outputs
    }, "pending outbox was not recovered"
    assert result.outbox_to_republish() == applied.outputs, "republish list mismatch"
    reporter.line(f"state_changes: {result.state_changes}")
    reporter.line(f"applied_inputs: {list(result.applied_inputs)}")
    reporter.line(f"done_inputs: {list(result.done_inputs)}")
    reporter.line(f"outbox_to_republish: {output_ids(result.outbox_to_republish())}")
    reporter.ok("pending outbox is recoverable")


def check_checkpoint_replay(
    state_dir: Path,
    applied: InputApplied,
    done: InputDone,
    reporter: "Reporter",
) -> None:
    reporter.scenario("replay after checkpoint")
    reporter.line("Write INPUT_APPLIED and INPUT_DONE, then replay after INPUT_APPLIED LSN.")
    reporter.line("Expected recovery: old state change is skipped, later DONE remains visible.")
    reporter.pause()

    wal = Wal(state_dir)
    lsn_applied = wal.append(applied)
    lsn_done = wal.append(done)
    wal.close()
    reporter.line(f"checkpoint_lsn: {lsn_applied}")
    reporter.line(f"later INPUT_DONE LSN: {lsn_done}")

    result = replay_result(state_dir, checkpoint_lsn=lsn_applied)
    assert result.state_changes == [], "checkpoint replay reapplied old state change"
    assert result.done_inputs == {done.msg_id: done}, "checkpoint replay missed done"
    reporter.line(f"state_changes after checkpoint: {result.state_changes}")
    reporter.line(f"done_inputs after checkpoint: {list(result.done_inputs)}")
    reporter.ok("checkpoint replay skips incorporated records")


def check_rotation(state_dir: Path, done: InputDone, reporter: "Reporter") -> None:
    reporter.scenario("WAL rotation")
    reporter.line("Write one record, rotate the WAL, then write a new record.")
    reporter.line("Expected recovery: wal.current starts fresh and wal.previous is kept.")
    reporter.pause()

    wal = Wal(state_dir)
    wal.append(done)
    reporter.line(f"before rotate files: {file_list(state_dir)}")
    wal.rotate()
    assert list(wal.replay(from_record=-1)) == [], "rotated WAL should start empty"
    reporter.line(f"after rotate files: {file_list(state_dir)}")

    new_done = InputDone("filter_usd_0:client_1:43", 1, 0, 43)
    wal.append(new_done)
    wal.close()

    assert (state_dir / "wal.previous").exists(), "rotation did not preserve previous WAL"
    assert replay_records(state_dir) == [
        new_done
    ], "new WAL after rotation did not replay expected record"
    reporter.line(f"final files: {file_list(state_dir)}")
    reporter.line(f"replayed current WAL records: {record_names(replay_records(state_dir))}")
    reporter.ok("rotation preserves previous WAL and starts a fresh current WAL")


def replay_records(state_dir: Path):
    wal = Wal(state_dir)
    try:
        return list(wal.replay(from_record=-1))
    finally:
        wal.close()


def replay_result(state_dir: Path, checkpoint_lsn: int):
    wal = Wal(state_dir)
    try:
        return WALReplayer(wal).replay(checkpoint_lsn=checkpoint_lsn)
    finally:
        wal.close()


def file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def file_list(path: Path) -> str:
    if not path.exists():
        return "[]"
    names = sorted(child.name for child in path.iterdir())
    return "[" + ", ".join(names) + "]"


def record_names(records) -> list[str]:
    return [type(record).__name__ for record in records]


def output_ids(entries: list[OutboxEntry]) -> list[str]:
    return [entry.output_id for entry in entries]


class Reporter:
    def __init__(self, step: bool, quiet: bool) -> None:
        self._step = step
        self._quiet = quiet

    def header(self, text: str) -> None:
        if self._quiet:
            print(f"wal_smoke | {text}")
            return
        print()
        print(f"== {text} ==")

    def scenario(self, text: str) -> None:
        if self._quiet:
            return
        print()
        print(f"-- {text} --")

    def line(self, text: str) -> None:
        if not self._quiet:
            print(text)

    def ok(self, text: str) -> None:
        print(f"OK | {text}")

    def pause(self) -> None:
        if self._step and not self._quiet:
            input("Press Enter to continue...")


if __name__ == "__main__":
    raise SystemExit(main())
