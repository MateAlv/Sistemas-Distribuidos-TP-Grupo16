# Query 4 — Scatter-Gather Pipeline

Query 4 detects the **scatter-gather** money-laundering pattern: a source
account `A` that fans money out to several intermediary accounts `M`, which then
funnel it into a single destination account `B` (`A → M → B`).

This document explains what the pipeline is and what each worker does. It is a
map for reviewers, not an implementation guide.

## What Q4 computes

Over **USD** transactions in the notebook raw-string window
`'2022/09/01' <= Timestamp <= '2022/09/06'`:

1. Keep only transactions whose **source account** sent to **more than 5
   distinct** target accounts (the "scatter" condition).
2. From those, build the two-hop paths `A → M → B` and drop `A == B`.
3. A pair `(A, B)` qualifies when **more than 5** such paths connect them.
4. The result is the set of **unique accounts** taking part in any qualifying
   pair, written as `Bank,Account`.

An account is identified by `(bank id, account number)`, with the bank id
normalized the way the reference notebook reads it (see
[bank_ids.py](../../src/common/bank_ids.py)).

## Pipeline at a glance

```text
DATE / USD filter
  → q4_source_prefilter   (sharded by source account A)
  → q4_edge_store         (sharded by intermediary account M)
  → q4_block_joiner       (one shard per join block; splits hot M)
  → q4_pair_reducer       (sharded by the pair (A, B))
  → q4_account_deduper    (sharded by account)
  → gateway               (writes Bank,Account)
```

Every stage is horizontally scalable: work is split across replicas by a hash of
that stage's key, so no single worker ever needs the whole dataset. State that
could grow large is capped or spilled to local temporary files, and released
once a client finishes.

## The stages

### DATE / USD filter
The shared filter forwards the USD transactions inside the Q4 date window. This
is Q4's only input; nothing downstream re-reads the source file.

### q4_source_prefilter
Owns every transaction of a given source account (input is sharded by `A`), so it
can apply the "sent to more than 5 distinct targets" rule exactly.

For each source it counts distinct targets only **up to 6** (enough to answer
"more than 5") and spools the source's rows to disk until the answer is known.
When a source reaches 6 distinct targets it is marked *qualified*, its spooled
rows are replayed downstream, and later rows stream straight through. Sources
that never qualify are dropped at end-of-stream.

Each qualified transaction `A → M` is forwarded as two **counted edges** keyed by
their intermediary — one recording that `M` receives from `A`, one recording that
`A` sends to `M`. Emitting both views is what lets the next stage rebuild paths.

### q4_edge_store
Collects counted edges for the intermediary accounts it owns (sharded by `M`) and
sums their counts into `incoming[M]` (who sends into `M`) and `outgoing[M]` (who
`M` sends to).

When the client's input is complete, it plans each `M`. A normal `M` becomes a
single join block. A **hot** `M` — a hub whose `incoming × outgoing` fan-out is
huge (think an account with thousands of counterparties) — is split into a grid
of smaller blocks so the join can be spread across many workers instead of
crushing one.

### q4_block_joiner
Runs the actual `A → M → B` join for one block. For each `(A, B)` reachable
through the block it computes the path weight `count(A→M) × count(M→B)`, skips
`A == B`, and emits a weighted contribution for the pair. Weights are capped at 6,
since "more than 5" is all that matters.

Splitting hot intermediaries into blocks here is what stops the pattern's worst
case — a single hub producing an enormous number of pairs — from landing on one
worker.

### q4_pair_reducer
Owns each `(A, B)` pair (sharded by the pair) and sums the weighted
contributions arriving from every block. As soon as a pair's total passes 5 it
qualifies: the reducer emits its two accounts `A` and `B` and forgets the pair.
Pairs that never reach the threshold are the bulk of the data, so this stage
keeps memory bounded by spilling to disk and merging at end-of-stream.

### q4_account_deduper
The same account can appear in many qualifying pairs. This stage (sharded by
account) emits each account only once, producing the final unique-account list.
It emits accounts to the gateway in `Q4AccountId` batches. Since the gateway
tracks one EOF per query queue, deduper shards report their emitted counts to
deduper `0`; only that leader sends the final Q4 gateway EOF after every shard
has flushed its batches.

### gateway
Collects the deduplicated accounts and writes the Q4 result as `Bank,Account`.

## End-of-stream and scaling

Every stage is replica-safe and uses the project's standard end-of-stream
handshake: a worker waits for an end-of-stream signal from each of its upstream
partitions, flushes whatever it has buffered or spooled, forwards its own
end-of-stream (carrying how much it sent to each downstream partition), and then
releases that client's state. The `sum`, `aggregator`, and `filter_q5_usd`
workers follow the same pattern.

## Records exchanged between stages

The binary records live in
[scatter_gather_serializer.py](../../src/common/message_protocol/internal/scatter_gather_serializer.py):

| Record | Carries |
| --- | --- |
| `Q4AccountId` | a `(bank id, account)` identity, and the final result row |
| `Q4TransactionEdge` | a prefilter's spooled `A → M` row |
| `Q4CountedEdge` | an incoming/outgoing edge for an intermediary `M`, with a count |
| `Q4BlockJoinEdge` | a counted edge assigned to a specific hot-`M` join block |
| `Q4PairDelta` | a weighted contribution to a pair `(A, B)` |
