# Q4 Scatter-Gather Overview

This document summarizes the protocol described in `docs/diseño-informe.pdf`
for Query 4. It is intentionally limited to the pipeline shape and the
responsibility of each stage.

## Query

Query 4 works over USD transactions in the range
`[2022-09-01, 2022-09-05]`.

The transaction stream is treated as a directed graph:

```text
A -> M -> B
```

`M` is the intermediate account. A result is emitted for a pair `(A, B)` when
there are at least 5 distinct intermediate accounts `M` connecting that same
origin and destination:

```text
A -> M1 -> B
A -> M2 -> B
A -> M3 -> B
A -> M4 -> B
A -> M5 -> B
```

The count is over distinct intermediate accounts for the same `(A, B)` pair.

## Pipeline

```text
Date-range filter
  -> Scatter-Gather Mapper
  -> Scatter-Gather Linker
  -> Scatter-Gather Detector, partitioned by (A, B)
  -> Gateway
```

The design report uses these stage names:

| Stage | Name |
| --- | --- |
| Mapper | `SGM` |
| Linker | `SGL` |
| Detector | `SGD` |

## Stage Responsibilities

### Date-Range Filter

The Q4 branch starts after the shared date-range filter. This filter sends the
transactions in the Q4 time window to the scatter-gather branch.

### Scatter-Gather Mapper

The mapper receives filtered transactions and routes them by the candidate
intermediate account `M`.

For a transaction `X -> Y`, the mapper has to preserve both possible roles:

```text
X -> Y can be an incoming edge to M=Y
X -> Y can be an outgoing edge from M=X
```

The mapper routes these records so every edge involving the same candidate `M`
is processed by the same linker partition. This is the shuffle by `hash(M)`
described in the design report.

### Scatter-Gather Linker

Each linker owns a partition of intermediate accounts. For each `M`, it links
incoming and outgoing edges into relations of the form:

```text
A -> M -> B
```

The linker keeps the state needed to join incoming and outgoing edges per `M`.
When it can form a distinct relation `(A, M, B)`, it forwards that row to the
detector partition selected by `hash(A, B)`. It should forward rows as early as
possible once the relation is known.

### Scatter-Gather Detector

Each detector owns a partition of `(A, B)` pairs. It receives distinct
`(A, M, B)` rows from the linker, grouped by `hash(A, B)`, and counts how many
different intermediate accounts were observed for each pair:

```text
count(distinct M for A, B) >= 5
```

When the detector reaches the threshold for a pair `(A, B)`, it marks that pair
as scatter-gather and sends `(A, B)` to the gateway immediately. It only needs
to remember which pairs were already emitted so the same result is not sent more
than once.

## Current Gap

At the design level, the missing Q4 work is the implementation of the three
scatter-gather stages and their wiring into the running pipeline.

In the current repository:

| Area | State |
| --- | --- |
| Q4 constants | Present |
| Scatter-gather worker directory | Present, but empty |
| Date filter output to Q4 branch | Present |
| Mapper, Linker, Detector logic | Missing |
| Runtime wiring for Q4 services | Missing |
| Gateway result handling for Q4 | Missing |

So the design is not far off as a protocol. The code is still before the Q4
implementation: the filtered transactions can reach the mapper queue, but the
scatter-gather branch itself is not implemented yet.
