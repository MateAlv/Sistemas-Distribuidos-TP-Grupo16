# Q4 — Scatter-Gather Pattern Detection: Protocol Design

## 1. Problem Statement

Query 4 detects accounts that act as **scatter-gather intermediaries**: an account M is a match if
at least **5 distinct** source accounts (A₁…Aₙ) each sent a USD transaction to M, **and** M sent at
least one transaction onward to some destination account B, all within the period
**2022-09-01 → 2022-09-05**.

Formally, find all M where:

```
|{ A : ∃ tx (from=A, to=M, USD, in range) }| ≥ 5
```

(The "gather" side — M→B — must also exist, but the cardinality threshold is on the scatter side.)

---

## 2. Pipeline Position

```
[Filter DATE]
      │
      ▼  SCATTER_GATHER_MAPPER_QUEUE
[Mapper (SGM × N)]     ← one per partition
      │  SG_LINKER_EXCHANGE (sharded by M-key)
      ▼
[Linker (SGL × N)]     ← one per partition
      │  SG_DETECTOR_QUEUE
      ▼
[Detector (SGD × 1)]
      │  GATEWAY_Q4_QUEUE
      ▼
[Gateway → Client]
```

The **Filter (C_DATE)** stage is already implemented: it emits every USD transaction in the
`[2022-09-01, 2022-09-05]` window to `SCATTER_GATHER_MAPPER_QUEUE` (see
`src/workers/filter/filters.py:280`).

---

## 3. Workers

### 3.1 Mapper (`C_SGM`)

**Role:** Partition routing — ensure that all edges involving the same intermediate node M land
on the same Linker replica.

**Key insight:** At routing time the Mapper does not know which account is M. Every transaction
`(from=A, to=B)` is simultaneously:
- A potential **scatter edge** A → M where **M = B** (`to_account`)
- A potential **gather edge** M → B where **M = A** (`from_account`)

Therefore the Mapper sends **each transaction twice**, tagged differently, to two potentially
distinct Linker partitions:

| Routing key | `to_account` | Tag |
|---|---|---|
| `hash(to_account) % N` | M-candidate | `SCATTER_EDGE` — treat `to_account` as M |
| `hash(from_account) % N` | M-candidate | `GATHER_EDGE` — treat `from_account` as M |

Both messages carry the full transaction payload. When both hashes resolve to the same partition
the message is sent once (deduplication is by content at the Linker level anyway).

**Input:** `SCATTER_GATHER_MAPPER_QUEUE`

**Output:** `SG_LINKER_EXCHANGE` (direct exchange, routing key = partition index as string)

**Sharding function:**
```python
def partition_for_account(account_id: int, n_partitions: int) -> int:
    return account_id % n_partitions  # accounts are already integer IDs
```

**EOF handling:** The Mapper uses the existing token-ring control protocol (identical to the
filter workers). After all data EOFs are flushed, it emits one EOF per Linker partition.

---

### 3.2 Linker (`C_SGL`)

**Role:** Per-partition graph builder. For its assigned set of M-candidates, accumulates:
- `scatter_sources[M]` = set of distinct A accounts that sent to M (`SCATTER_EDGE` messages)
- `gather_targets[M]` = set of distinct B accounts that M sent to (`GATHER_EDGE` messages)

At EOF, emits every M where `len(scatter_sources[M]) ≥ 5`.

**State per client:**
```python
scatter_sources: dict[int, set[int]]   # M → {A₁, A₂, …}
gather_targets:  dict[int, set[int]]   # M → {B₁, B₂, …}
```

**Processing per message:**
```
SCATTER_EDGE (from=A, to=M):
    scatter_sources[M].add(A)

GATHER_EDGE (from=M, to=B):
    gather_targets[M].add(B)
```

**At EOF (per client):**
```
for M, sources in scatter_sources[client].items():
    if len(sources) >= 5 and M in gather_targets[client]:
        emit (M, len(sources))  →  SG_DETECTOR_QUEUE
```

**Input:** `SG_LINKER_EXCHANGE` partition `i` → queue `sg_linker_i`

**Output:** `SG_DETECTOR_QUEUE`

**EOF handling:** Each Linker partition emits an EOF to `SG_DETECTOR_QUEUE` carrying its own
`msgs_sent` count. The Detector accumulates `N_linkers` EOFs before declaring the stream done.

---

### 3.3 Detector (`SGD`)

**Role:** Final aggregation — collects the qualifying M accounts emitted by all Linker replicas
and forwards them to the gateway.

Because Linker partitions are disjoint by M-key, **no deduplication is needed**. The Detector
simply fans in all qualifying accounts, counts how many EOFs it has received from Linkers, and
when all N linker EOFs arrive it sends the results EOF to the gateway.

**State per client:**
```python
results: list[int]   # matching M account IDs
eof_count: int
```

**Output:** `GATEWAY_Q4_QUEUE`

---

## 4. Message Format

All internal messages follow the existing `InternalProtocol` packet layout:

```
[ msg_type (1B) | client_id (16B) | payload ]
```

### 4.1 Mapper → Linker payload (DATA)

The existing `TransactionSerializer` binary format is reused, with one additional byte prepended
to carry the edge tag:

```
[ edge_tag (1B) | TransactionSerializer.serialize(tx) ]
```

| `edge_tag` | Value | Meaning |
|---|---|---|
| `SCATTER_EDGE` | `0x01` | `to_account` is M |
| `GATHER_EDGE`  | `0x02` | `from_account` is M |

### 4.2 Linker → Detector payload (DATA)

```
[ account_id (8B, big-endian uint64) | scatter_count (4B, big-endian uint32) ]
```

### 4.3 Detector → Gateway payload (DATA)

```
[ account_id (8B, big-endian uint64) ]
```

Same as Q1/Q2 results — the gateway already handles a stream of result records keyed by client.

---

## 5. EOF / Control Flow

The existing token-ring EOF protocol used by the filter workers applies unchanged to each stage.
The key counts:

| Stage | `FILTER_AMOUNT` env var | Sends EOF to |
|---|---|---|
| Mapper (N replicas) | `SG_MAPPER_AMOUNT` | N Linker queues (one EOF each) |
| Linker (N replicas) | `SG_LINKER_AMOUNT` | `SG_DETECTOR_QUEUE` (N EOFs total) |
| Detector (1 replica) | `1` | `GATEWAY_Q4_QUEUE` |

The Detector must wait for **`SG_LINKER_AMOUNT`** EOFs before forwarding its own EOF upstream.
This is equivalent to how the existing `Joiner` waits for `AGGREGATION_AMOUNT` EOFs.

---

## 6. Implementation Gap Analysis

### What is already done

| Component | Status | Location |
|---|---|---|
| Filter (C_DATE) routes to `SCATTER_GATHER_MAPPER_QUEUE` | ✅ Done | `src/workers/filter/filters.py:280` |
| Constants `C_SGM`, `C_SGL`, `C_SGD` | ✅ Done | `src/common/constants.py` |
| `src/workers/scatter_gather/` directory + Dockerfile | ✅ Skeleton | `src/workers/scatter_gather/` |
| Internal protocol (packet header, serializer) | ✅ Done | `src/common/message_protocol/internal.py` |
| RabbitMQ middleware (queues + direct exchanges) | ✅ Done | `src/common/middleware/middleware_rabbitmq.py` |
| Token-ring EOF control protocol | ✅ Done | `src/workers/filter/filters.py` (reference impl) |

### What is missing

| Component | Status | Notes |
|---|---|---|
| `scatter_gather.py` — Mapper logic | ❌ Empty | Hash both accounts, send with edge tag |
| `scatter_gather.py` — Linker logic | ❌ Empty | Accumulate sets per M, emit at EOF |
| `scatter_gather.py` — Detector logic | ❌ Empty | Fan-in from N linkers, emit to gateway |
| `main.py` for scatter_gather | ❌ Empty | Wire env vars, instantiate correct worker type |
| `Transaction.hash_by_account` helper | ⚠️ Partial | `hash_by_payment_format` exists; need account variant |
| Edge-tag in payload (byte prepend) | ❌ Missing | New serialization convention for Mapper→Linker |
| `SG_LINKER_EXCHANGE` declaration | ❌ Missing | Direct exchange, N routing keys |
| `SG_DETECTOR_QUEUE` declaration | ❌ Missing | Single queue consumed by Detector |
| `GATEWAY_Q4_QUEUE` declaration + gateway receive | ❌ Missing | Gateway `_wait_for_results` is a stub |
| docker-compose entries for SGM/SGL/SGD | ❌ Missing | N mapper, N linker, 1 detector |
| Env vars for SGM/SGL/SGD | ❌ Missing | See §7 below |

---

## 7. Environment Variables (proposed)

### Mapper (`C_SGM`)

| Variable | Example | Description |
|---|---|---|
| `CONFIGURATION` | `SGM` | Worker type selector |
| `ID` | `0` | Replica index |
| `MOM_HOST` | `rabbitmq` | RabbitMQ host |
| `INPUT_QUEUE` | `sg_mapper_queue` | Queue fed by Filter (C_DATE) |
| `SG_LINKER_EXCHANGE` | `sg_linker_exchange` | Direct exchange for Linker sharding |
| `SG_LINKER_AMOUNT` | `2` | Number of Linker partitions |
| `SG_MAPPER_AMOUNT` | `2` | Total Mapper replicas (for token ring) |
| `SG_MAPPER_PREFIX` | `sg_mapper` | Exchange prefix for control ring |

### Linker (`C_SGL`)

| Variable | Example | Description |
|---|---|---|
| `CONFIGURATION` | `SGL` | Worker type selector |
| `ID` | `0` | Partition index |
| `MOM_HOST` | `rabbitmq` | RabbitMQ host |
| `SG_LINKER_EXCHANGE` | `sg_linker_exchange` | Input exchange |
| `SG_DETECTOR_QUEUE` | `sg_detector_queue` | Output queue to Detector |
| `SG_LINKER_AMOUNT` | `2` | Total Linker replicas |
| `SG_LINKER_PREFIX` | `sg_linker` | Control ring prefix |
| `MIN_SCATTER_COUNT` | `5` | Minimum distinct sources to qualify |

### Detector (`C_SGD`)

| Variable | Example | Description |
|---|---|---|
| `CONFIGURATION` | `SGD` | Worker type selector |
| `MOM_HOST` | `rabbitmq` | RabbitMQ host |
| `SG_DETECTOR_QUEUE` | `sg_detector_queue` | Input queue |
| `GATEWAY_Q4_QUEUE` | `gateway_q4_queue` | Output to gateway |
| `SG_LINKER_AMOUNT` | `2` | How many EOFs to wait for |

---

## 8. Recommended Implementation Order

1. **Add `hash_by_account(n)` to `Transaction`** — mirrors the existing `hash_by_payment_format`.
2. **Implement Mapper** in `scatter_gather.py` — dual-routing with edge tag byte.
3. **Implement Linker** in `scatter_gather.py` — set accumulation, emit qualifying M at EOF.
4. **Implement Detector** in `scatter_gather.py` — fan-in, forward to gateway queue.
5. **Wire `main.py`** — env-var dispatch to the correct worker class.
6. **Add gateway Q4 result handling** — `_wait_for_results` currently logs
   `status=not_implemented` (`src/gateway/gateway.py:176`); extend it to read from
   `GATEWAY_Q4_QUEUE` and stream results back to the client.
7. **Add docker-compose entries** — at minimum: 1 mapper, 1 linker, 1 detector for smoke testing.
8. **Validate against reference notebook** output for LI-Mini dataset.
