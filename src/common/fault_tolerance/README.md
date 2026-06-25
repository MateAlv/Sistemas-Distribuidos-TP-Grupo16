# Fault tolerance — durable worker state

This package gives a worker the ability to crash at any moment and recover
without losing or double-applying a message. It solves the **disk-persistence**
half of the fault-tolerance design. The WAL-specific notes live in
[docs/fault-tolerance/wal.md](../../../docs/fault-tolerance/wal.md).
Restarting crashed containers (monitors, heartbeats, leader election) is
handled elsewhere.

The guarantee it provides:

- **No confirmed message is lost.** State and pending outputs are on disk before
  the worker acks RabbitMQ.
- **No message is applied twice.** Every input is deduplicated against durable
  state, so a redelivery after a crash is detected and dropped.

Duplicates may still be *published* downstream on recovery; that is expected and
is absorbed by the downstream worker's own deduplication.

## Basic Idea

A worker processes one input message at a time. For each input it does three
durable steps, in this order:

1. **Apply** — compute the state change and the outputs, write them to the WAL,
   then apply the change in memory. (`INPUT_APPLIED`)
2. **Publish** — send the outputs to RabbitMQ and wait for publisher confirms.
3. **Commit** — record that the input is finished, drop its outputs, ack
   RabbitMQ. (`INPUT_DONE`)

Disk is written *before* memory is mutated and *before* RabbitMQ is acked, so a
crash between any two steps is recoverable. To keep the WAL from growing forever,
the worker periodically writes a full **snapshot** and starts a fresh WAL.

On startup the worker loads the latest snapshot, replays the WAL on top of it,
and re-publishes anything still pending — then resumes consuming.

## Two planes

The code is split into two concerns:

- **Messaging plane** (handler-owned): which inputs were seen, which outputs are
  in flight, and the ids stamped on outgoing messages. This is the same for
  every worker. Lives in `inbox/`, `outbox/`, `handler/sender_sequencer.py`.
- **Business plane** (worker-owned): the actual aggregation/join/count state.
  Each worker supplies a small adapter implementing the `WorkerState` protocol.

The `PersistentStateHandler` orchestrates both and owns the ordering of every
disk write, memory mutation and ack.

## Directory layout

```
fault_tolerance/
  worker_state.py          WorkerState protocol (the business-plane contract)
  _encoding.py             shared binary read/write helpers

  handler/
    persistent_state_handler.py   the orchestrator the worker loop talks to
    worker_runner.py              the consume loop that drives the handler
    sender_sequencer.py           stamps durable ids on outgoing messages
    action.py                     Action enum returned to the loop
    worker_loop_instruction.py    what handle() returns: action + outputs + ctx

  inbox/
    inbox.py                 per-client dedup: NEW / APPLIED / DONE
    inbox_status.py          the InboxStatus enum
    msg_kind.py              MsgKind enum: separates data and control streams
    deduplication_tracker.py bounded "seen seqs" tracker (biggest + gaps)

  outbox/
    outbox.py                pending outputs grouped by client and input
    outbox_entry.py          one outgoing message (id, destination, body)

  snapshot/
    snapshot.py              the full-state record written to disk
    last_state.py            atomic double-buffered snapshot file store

  wal/
    wal.py                   append-only log facade (append / replay / rotate)
    writer.py                serializes + fsyncs one record
    reader.py                decodes records back from bytes
    record.py                RecordType enum + on-disk header format
    input_applied.py         INPUT_APPLIED record
    input_done.py            INPUT_DONE record
    wal_record.py            WalRecord = InputApplied | InputDone
```

## The contract a worker implements: `WorkerState`

`worker_state.py` defines a `Protocol` with three methods. A worker's adapter
holds the business state and knows how to serialize and replay it:

- `snapshot() -> dict` — return the full business state as a picklable dict.
- `restore(data: dict)` — load state from a snapshot dict (`{}` means fresh).
- `apply_change(change: dict)` — apply one change. This is the **single** code
  path that mutates business state, used both live and during replay, so replay
  reproduces the live result exactly.

The `change` dict is whatever the worker decides, with one constraint: it is
serialized as JSON in the WAL, so it must be JSON-safe (e.g. raw bytes are
base64-encoded). See `workers/aggregator/aggregator_state.py` for a worked
example.

## Handler

### `PersistentStateHandler`

The only object the worker loop talks to. It owns the inbox, outbox, sequencer,
WAL and snapshot store, plus the worker's `WorkerState`.

Main methods:

- `recover()` — load the snapshot, replay the WAL on top, restore the sequencer.
  Call once at startup before consuming.
- `outbox_to_republish()` — the outputs to resend after recovery.
- `handle(msg_id, client_id, sender_id, seq, payload, business_fn)` — process one
  input. Returns a `WorkerLoopInstruction`.
- `commit_done(msg_id, client_id, sender_id, seq, kind)` — finish an input after
  its outputs are confirmed: write `INPUT_DONE`, drop the outputs, maybe snapshot.
- `maybe_snapshot()` — snapshot + rotate the WAL once enough inputs have been
  applied since the last one.
- `snapshot_now()` — snapshot + rotate immediately, used to persist state mutated
  outside the WAL (e.g. the EOF coordinator) before acking its message.

`kind` (a `MsgKind`) tags each message as data or a specific control message, and
defaults to `DATA`. It is part of the dedup key so a control message can never
collide with a data message that happens to share the same `(sender_id, seq)`.

`business_fn` is supplied per message and returns
`(state_change: dict, outputs: list[(destination, body)])`. It returns *logical*
outputs; the handler stamps their ids.

What `handle()` does for a brand-new input:

```
state_change, logical_outputs = business_fn(payload)
outputs = sequencer.stamp(...)        # assign durable ids
wal.append(INPUT_APPLIED(...))        # fsync to disk
sequencer.observe(outputs)            # memory mutation, after the WAL
worker_state.apply_change(...)
inbox.mark_applied(...)
outbox.add(...)
return PUBLISH_THEN_COMMIT with the outputs
```

If the input is already `APPLIED` (a redelivery after a crash), it skips the
business logic and just re-asks to publish the stored outputs. If it is `DONE`,
it returns `ACK`.

**Recovery is idempotent.** `recover()` replays the WAL through `_apply_record`,
which consults the inbox and skips any record already reflected in the loaded
snapshot. This is why precise WAL checkpoint offsets are not needed: we rotate
the WAL after every snapshot (`REPLAY_ALL = -1` replays the whole segment) and
the inbox guard prevents double-application.

### `WorkerRunner`

The handler owns disk and memory but not the RabbitMQ channel, so `WorkerRunner`
is the consume loop that drives it. At startup it calls `recover()` and
re-publishes the outbox. For each message it unpacks the addressed packet, runs
`handle()`, and then publishes the outputs, calls `commit_done()` and acks. Any
failure nacks with requeue so RabbitMQ redelivers; recovery makes that safe. It
shares the worker's lock with other consumers (e.g. an EOF coordinator) and
publishes outside the lock, since publishing is network I/O.

### `Action` and `WorkerLoopInstruction`

`handle()`/`commit_done()` return a `WorkerLoopInstruction` that tells the loop
what to do next:

- `Action.ACK` — nothing to publish, just ack RabbitMQ.
- `Action.PUBLISH_THEN_COMMIT` — publish `instruction.outputs`, wait for
  confirms, then call `commit_done(*instruction.ctx)`.

`ctx` is the input's `(msg_id, client_id, sender_id, seq, kind)`.

### `SenderSequencer`

Assigns the id every outgoing message carries, in one place. There are two modes,
chosen per destination:

- **Plain** — for a destination with no `EdgeSpec`. The body is left untouched and
  the id is `"{node_id}:{client_id}:{seq}#{index}"`, where `seq` is a per-client
  counter and `index` the output's position within its input's batch.
- **Addressed** — for a destination with an `EdgeSpec`. The body is rebuilt as an
  addressed packet carrying `sender_id` and `seq`, so the consumer can deduplicate
  on `(client, kind, sender_id, seq)`. The id is
  `"{node_id}:{client_id}:{edge}:{shard}:{seq}#{index}"`.

An `EdgeSpec` holds the producer's `sender_id` and the destination's `shard_count`.
The addressed `seq` is counted per `(client, edge, shard)`, not per client, because
the edge shards by payload digest: a single per-client counter would leave each
shard's dedup tracker full of gaps.

Counters are persisted in the snapshot, so they never reset on restart and an id is
never reused for different content. `stamp()` builds ids from the current counters
without advancing them; the handler calls `observe()` only after the WAL append, so
a crash mid-append leaves no gap. On recovery `observe()` rebuilds the counters from
the ids in replayed records, so new ids never collide with persisted ones.

## Inbox — input deduplication

### `Inbox`

Tracks, per client, the state of every input. `classify(client, sender, seq, kind)`
returns an `InboxStatus`:

- `NEW` — never seen; process it.
- `APPLIED` — state changed but outputs not yet published and committed.
- `DONE` — fully finished; drop as a duplicate.

It holds two structures, both keyed with `kind` so data and control messages stay
separate:

- `applied`: the set of `(kind, sender_id, seq)` triples currently mid-flight, per
  client. Small — entries move out as inputs commit.
- `done`: a `DeduplicationTracker` per `(client_id, kind, sender_id)`, the
  bounded-memory record of everything already finished.

`mark_applied` / `mark_done` drive the transitions; `drop_client` forgets a
client once it closes.

### `DeduplicationTracker`

The bounded "have I seen this seq?" structure for one `(client, kind, sender)`
key. Instead of storing every seq ever seen, it keeps:

- `biggest` — the highest seq seen so far.
- `pending` — the gaps below `biggest` that have not arrived yet.

A seq is a duplicate when `seq <= biggest` **and** `seq not in pending`.

```
observe 1, 2, 5   ->  biggest = 5, pending = {3, 4}
is_duplicate(2)   ->  True   (2 <= 5, not pending)
is_duplicate(3)   ->  False  (3 is a pending gap, i.e. expected)
observe 3         ->  biggest = 5, pending = {4}
```

Memory stays bounded because gaps fill in as messages arrive, and the whole
tracker is dropped when the client closes.

## Outbox — pending outputs

### `Outbox`

Holds outputs that have been computed but not yet confirmed downstream, grouped
`outbox[client_id][input_id] -> [OutboxEntry]`. On recovery, everything still in
the outbox is re-published (`all_pending()`), because we may have crashed before
the confirms arrived.

- `add(client, input_id, entries)` — record an input's outputs.
- `entries_for_input(client, input_id)` — what to publish for one input.
- `remove_input(client, input_id)` — drop them once the input commits (prunes the
  client entry when empty).
- `all_pending()` / `drop_client(client)`.

### `OutboxEntry`

One outgoing message:

- `output_id` — the deterministic id from the sequencer.
- `input_id` — the input that produced it.
- `destination` — the publisher-registry key (logical edge or queue name).
- `body` — the fully serialized message, resent byte-for-byte on recovery.
- `shard` — the destination partition to route to, or `None` to let the publisher
  route.

It serializes itself to a compact binary form (used inside WAL records).

## Snapshot — the periodic full-state checkpoint

### `Snapshot`

A plain dataclass holding everything needed to reconstruct the worker:
`wal_checkpoint_record`, `inbox`, `outbox`, `sequencer` (all handler-owned), and
`worker_state` (the business adapter's dict). Sections are plain picklable data.

### `LastState`

Reads and writes the snapshot on disk with a **double buffer** so a crash mid-
write never destroys the last good state:

```
write snapshot -> last_state.tmp ; fsync
rename last_state.current -> last_state.previous   (atomic)
rename last_state.tmp     -> last_state.current     (atomic)
fsync the directory
```

`load()` returns `current`, falling back to `previous` if `current` is missing or
corrupt, or `None` if neither exists. `commit()` must fully succeed before the
WAL is rotated, so the snapshot and the WAL never disagree.

## WAL — the write-ahead log

An append-only file of records written since the last snapshot. Each record is
fsynced before the worker proceeds, so it survives a crash.

### `Wal`

The facade the handler uses:

- `append(record)` — write one `InputApplied` / `InputDone` and fsync.
- `replay(from_record)` — yield the records back for recovery.
- `rotate()` — after a snapshot, move `wal.current` aside and start an empty one.

### Records: `InputApplied`, `InputDone`, `WalRecord`

- `InputApplied(msg_id, client_id, sender_id, seq, state_change, outputs, kind)` —
  the apply step: carries the business change (JSON) and the stamped outputs.
- `InputDone(msg_id, client_id, sender_id, seq, kind)` — the commit step.
- `WalRecord` is the union `InputApplied | InputDone`.

### `WALWriter` / `WALReader` / `record.py`

- `WALWriter` serializes a record (binary framing for ids and outputs, JSON for
  the state change), writes it, and fsyncs. Returns the byte offset (LSN).
- `WALReader` reads the file back, stopping cleanly at a half-written trailing
  record (a crash mid-append), and decodes each record.
- `record.py` defines the on-disk header (`>BI`: a 1-byte type + 4-byte length)
  and the `RecordType` enum (`INPUT_APPLIED`, `INPUT_DONE`).

## Shared helpers: `_encoding.py`

Small functions for length-prefixed binary reads/writes (`read_uint16`,
`read_length_prefixed_u32`, etc.) shared by the WAL and `OutboxEntry`. Nothing
domain-specific; just bounds-checked struct parsing.

## How a crash is handled at each step

| Crash point | RabbitMQ redelivers | On recovery |
|---|---|---|
| Before `INPUT_APPLIED` is fsynced | yes | reprocess as new (nothing on disk) |
| After `INPUT_APPLIED`, before publishing | yes | inbox says `APPLIED`; re-publish the outbox, then commit |
| While publishing / before `INPUT_DONE` | yes | re-publish the full outbox; downstream deduplicates; then commit |
| After `INPUT_DONE`, before the ack | yes | inbox says `DONE`; ack immediately, no reprocessing |
| Mid-snapshot (`.current` moved, new not written) | — | `LastState` falls back to `.previous`; WAL still has the rest |
