# Q4 Distributed Scatter-Gather Plan

This document describes the new Query 4 architecture. It replaces the old
distinct-intermediary pair detector with a notebook-exact distributed join.

## Notebook Semantics

Query 4 uses USD transactions whose raw `Timestamp` string satisfies:

```text
'2022/09/01' <= Timestamp <= '2022/09/06'
```

Because the notebook compares strings directly, rows such as
`2022/09/06 00:00` are outside the Q4 window.

Account identity is always:

```text
(notebook_bank_id(bank), account)
```

The query is:

```text
T  = Q4-window USD transactions
Tq = rows in T whose source A=(From Bank, Account)
     sent to more than 5 distinct targets M=(To Bank, Account.1)

pairs = Tq as A->M joined with Tq as M->B
drop A == B
group by (A, B)
keep groups with path-row count > 5

output unique accounts from every qualifying A and B
```

The count is row-combination multiplicity, not distinct intermediaries:

```text
weight(A, B, M) = count(A->M) * count(M->B)
pair_count(A, B) = sum_M weight(A, B, M)
qualifies iff pair_count(A, B) > 5
```

Weights are non-negative, so workers may cap accumulated counts at `6`.

## Pipeline

```text
DATE/USD Q4 filter
  -> q4_source_prefilter, sharded by source A
  -> q4_edge_store, sharded by intermediary M
  -> q4_block_joiner, salted by hot M blocks
  -> q4_pair_reducer, sharded by (A, B)
  -> q4_account_deduper, sharded by account
  -> gateway
```

Every stage streams records. No stage should load the full input file in
memory. Large per-client state must either be capped, compacted, or spilled to
local temporary files and released when the client finishes.

## Message Contracts

The shared binary DTOs live in
`common.message_protocol.internal.scatter_gather_serializer`.

| DTO | Purpose |
| --- | --- |
| `Q4AccountId` | `(bank_id, account)` identity and final result row. |
| `Q4TransactionEdge` | Qualified transaction edge `A -> M`. |
| `Q4CountedEdge` | Counted `IN` or `OUT` edge grouped by intermediary `M`. |
| `Q4BlockJoinEdge` | Counted edge assigned to one hot-M join block. |
| `Q4PairDelta` | Weighted `(A, B)` contribution, capped at 6. |

Old `ScatterGatherRelation` and `ScatterGatherResult` remain in the codebase
only for the current legacy workers. New Q4 workers should use the Q4 DTOs.

Use `partition_for_parts(...)` from `common.message_protocol.internal` for
composite keys such as `(bank, account)`, `(A, B)`, and hot-M block ids. Direct
exchange publishers can send to a computed partition with:

```text
exchange.send(packet, routing_key=key)
```

That keeps sharding local to each emitted record and avoids one publisher
instance per downstream partition.

When this document says `hash(A)`, `hash(B)`, or `hash(account)`, the hashed
value is the full `Q4AccountId`: bank id plus account id.

## Stage Responsibilities

### Q4 Filter

The existing DATE filter should forward only Q4-window rows using the notebook
raw timestamp comparison:

```text
'2022/09/01' <= Timestamp <= '2022/09/06'
```

The next implementation step should emit compact `Q4TransactionEdge` records
instead of full `Transaction` rows once the new prefilter worker is wired.

### Source Prefilter

Input is sharded by source account:

```text
routing_key = q4_prefilter_<hash(A) % N>
```

Each worker owns complete state for its sources. For every source `A`, it keeps:

```text
distinct_targets_capped_at_6
qualified_flag
pending_rows_spool
```

Rules:

1. Count distinct targets only up to 6.
2. Before `A` qualifies, append rows for `A` to a local per-client spool.
3. When `A` reaches 6 distinct targets, mark it qualified and replay its
   pending rows downstream.
4. After `A` qualifies, stream its later rows immediately.
5. At EOF, discard pending rows for sources that never qualified.

This implements the notebook `groupby(["From Bank", "Account"]).filter(...)`
without centralizing the file.

### Edge Store

Qualified rows are routed by intermediary `M`.

Every qualified transaction edge `source -> target` must be routed twice, once
for each side of the self-join:

```text
IN edge:  intermediary = target, endpoint = source
OUT edge: intermediary = source, endpoint = target
```

The edge store preserves multiplicity by counting both views:

```text
incoming[target][source] += 1
outgoing[source][target] += 1
```

At EOF the edge store plans each intermediary:

```text
in_size = number of A endpoints for M
out_size = number of B endpoints for M
estimated_pairs = in_size * out_size
```

Small `M` values use one block. Hot `M` values are split into block joins.

### Hot-M Block Join

For hot intermediaries, do not route all of `M` to one worker. Split the join:

```text
a_bucket = hash(A) % SA
b_bucket = hash(B) % SB
block = (M, a_bucket, b_bucket)
```

Correct fan-out is:

```text
incoming A for hot M -> every block (M, a_bucket(A), *)
outgoing B for hot M -> every block (M, *, b_bucket(B))
```

This duplicates the smaller side across a bounded number of blocks, but it
distributes the Cartesian product for skewed intermediaries.

Each block joiner computes:

```text
weight = count(A->M) * count(M->B)
```

Before emitting anything, it must drop `A == B`.

If `weight > 5`, the pair already qualifies from this one intermediary. The
joiner can emit a capped `Q4PairDelta(A, B, 6)`. Otherwise it emits
`Q4PairDelta(A, B, weight)`.

### Pair Reducer

Pair reducers are sharded by `(A, B)`:

```text
routing_key = q4_pair_<hash(A, B) % N>
```

They accumulate:

```text
pair_count[A,B] = min(6, pair_count[A,B] + delta.weight)
```

Once the count reaches 6, emit two `Q4AccountId` candidates, `A` and `B`, and
drop the pair counter.

The reducer must be spillable. When its in-memory map reaches a configured
limit, flush sorted `(A, B, count)` runs to disk. At EOF, merge the runs and
finish the threshold check.

### Account Deduper

Account candidates are sharded by account:

```text
routing_key = q4_account_<hash(bank, account) % N>
```

Each deduper emits each `Q4AccountId` once to the gateway. It may keep a set in
memory for small datasets, but the production path should support sorted-run
spill and merge at EOF.

The gateway writes:

```text
Bank,Account
```

## EOF and Scaling Contract

Every Q4 worker group must support multiple replicas and exact EOF propagation.
Use the same leader-gather pattern already used by `sum`, `aggregator`,
`filter_q5_usd`, and the current scatter-gather workers:

1. Upstream sends EOF with the number of records it forwarded to the group.
2. If the group has one replica, the worker flushes local buffers, emits all
   downstream data, sends downstream EOF with its own forwarded count, and
   releases client state.
3. If the group has multiple replicas, upstream EOF is broadcast to the group.
   Each replica snapshots processed and forwarded counts while holding its
   lock, flushes buffered/spooled data that belongs before EOF, and reports the
   snapshot to the leader.
4. The leader waits until reported processed counts reach the upstream
   expected total, then emits a flush order.
5. Every replica sends downstream EOF carrying the number of records it emitted
   to each downstream partition.
6. Downstream stages wait for one EOF from each upstream partition before
   closing the client.

Late data after EOF snapshot must be handled like the existing scalable
workers: process it, flush it ahead of close, and report a delta to the leader.

Good implementation references:

```text
src/workers/sum/sums.py
src/workers/aggregator/aggregators.py
src/workers/filter_q5_usd/filter_q5_usd.py
src/workers/q2_bank_name_joiner/bank_name_joiner.py
```

The new Q4 workers should copy that control shape instead of inventing a local
EOF shortcut. The data state will be different, but the ordered sequence is the
same: snapshot, leader wait, flush order, downstream EOF, local cleanup.

## Implementation Order

1. Keep the notebook-exact Q4 reference and LI-Mini fixture as validation.
2. Add the new Q4 DTOs and dynamic routing helper.
3. Implement source-prefilter workers and tests.
4. Implement edge-store workers without hot-M salting.
5. Implement pair reducer and account deduper.
6. Switch gateway/client to the Q4 account result contract.
7. Pass synthetic and LI-Mini end-to-end Q4.
8. Add hot-M block planning and block joiners.
9. Add spill paths for pair reducers and account dedupers.
10. Run LI-Small and then larger datasets.
