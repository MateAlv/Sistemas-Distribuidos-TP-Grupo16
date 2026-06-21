# Aggregator — Flujo interno

El aggregator recibe parciales de los Sum workers, los reduce en un resultado
final por cliente, y emite ese resultado downstream cuando todos los shards
confirmaron que terminaron de procesar.

---

## Archivos

| Archivo | Rol |
|---|---|
| `main.py` | Entry point. Instancia `AggregatorWorker` y registra el handler de SIGTERM. |
| `aggregators.py` | Lógica del worker: tres threads (datos, control, respuesta) + wiring con `PersistentStateHandler`. |
| `aggregator_state.py` | Adaptador `WorkerState`: encapsula el estado de negocio y lo expone via `snapshot/restore/apply_change`. |
| `processors.py` | Reducción pura por configuración (`Q2AggregatorProcessor`, `Q3AggregatorProcessor`, `Q5AggregatorProcessor`). |

---

## Variables de entorno

| Variable | Descripción |
|---|---|
| `ID` | Índice de esta instancia (0, 1, 2, …). La instancia 0 es el **líder** en el protocolo de coordinación. |
| `CONFIGURATION` | `Q2`, `Q3` o `Q5`. Determina el processor a usar. |
| `AGGREGATION_PREFIX` | Prefijo del exchange de entrada y de las colas de control/respuesta. |
| `AGGREGATION_AMOUNT` | Total de réplicas del aggregator (N). |
| `OUTPUT_QUEUE` | Cola downstream donde se emiten los resultados. |
| `STATE_DIR` | Directorio para WAL y snapshots. Default: `/tmp/aggregator_state`. |
| `SNAPSHOT_INTERVAL` | Cada cuántos mensajes aplicados se genera un snapshot. Default: `1000`. |

---

## Arquitectura de threads

```
┌───────────────────────────────────────────────────────┐
│ Thread datos (main)                                   │
│  Exchange: AGGREGATION_PREFIX_{ID}                    │
│  Procesa mensajes con sender_id+seq (addressed)       │
│  → DATA: acumula en processor via AggregatorState     │
│  → EOF:  reporta al líder (N>1) o emite directo (N=1) │
└───────────────────────┬───────────────────────────────┘
                        │ lock
┌───────────────────────▼───────────────────────────────┐
│ Thread control (todos los ID)                         │
│  Cola: AGGREGATION_PREFIX_control_{ID}                │
│  → FLUSH_ORDER (no-líder): emite resultados + FLUSH_ACK │
│  → FLUSH_ORDER (líder):    ignorado (no-op)           │
└───────────────────────────────────────────────────────┘
                        │ lock
┌───────────────────────▼───────────────────────────────┐
│ Thread respuesta (solo líder, ID == 0)                │
│  Cola: AGGREGATION_PREFIX_response_{ID}               │
│  → PROCESSED_ANSWER: acumula reportes → FLUSH_ORDER   │
│  → FLUSH_ACK:        acumula ACKs → líder emite       │
└───────────────────────────────────────────────────────┘
```

Los tres threads comparten `self._lock`. Todo acceso a `AggregatorState` o
`EofCoordinator` ocurre bajo ese lock.

---

## Protocolo de coordinación EOF (modo `flush_order`)

El aggregator usa `EofCoordinator` en modo **flush_order**: cada réplica tiene
su propio shard de entrada y el líder es siempre `ID=0`.

```
Shard 0 (líder)        Shard 1 (no-líder)       Shard 2 (no-líder)
     │                       │                        │
     │ EOF upstream           │ EOF upstream           │ EOF upstream
     ▼                       ▼                        ▼
 SendAnswerAction →  SendAnswerAction →         SendAnswerAction →
     │  PROCESSED_ANSWER      │  PROCESSED_ANSWER      │  PROCESSED_ANSWER
     └──────────────────────►líder◄──────────────────────┘
                              │  (acumula N reportes)
                              │
                   N reportes completos?
                         sí → FLUSH_ORDER a todos
                              │
              ┌───────────────┼────────────────┐
              ▼               ▼                ▼
         shard 0          shard 1          shard 2
       (ignora)       FlushAction(no-ldr) FlushAction(no-ldr)
                       emite resultados    emite resultados
                       envía FLUSH_ACK     envía FLUSH_ACK
                              │                │
                              └───────────────►│
                                              líder
                                    (acumula N-1 FLUSH_ACKs)
                                              │
                                    FlushAction(is_leader=True)
                                    líder emite sus resultados
```

**Caso N=1:** el líder emite sus resultados directamente desde el thread de datos
al recibir el EOF upstream, sin pasar por el protocolo de coordinación.

---

## Flujo de mensajes por tipo

### DATA (thread de datos)
```
unpack_addressed_packet(msg)
    → msg_type=DATA, client_id, sender_id, seq, payload

handle(msg_id, client_id, sender_id, seq, payload,
    bfn=lambda: (data_change(client_id, payload), []))
    │
    ├── WAL: InputApplied (state_change=data_change, outputs=[])
    ├── apply_change → processor.accept(payload) + data_count++
    └── returns WorkerLoopInstruction (outputs=[])

commit_done()  →  WAL: InputDone
ack()
```

### EOF upstream (thread de datos)
```
unpack_addressed_packet(msg)
    → msg_type=EOF, client_id, sender_id, seq, payload

handle(msg_id, ..., bfn):
  bfn llama coordinator.on_upstream_eof(...)    ← primera llamada (live)
  apply_change llama on_upstream_eof otra vez   ← segunda llamada (replay, no-op
                                                   porque seen_eof ya tiene client_id)
  
  Si N>1 → SendAnswerAction:
    state_change = coordinator_upstream_eof_change
    outputs = [(response_queue_líder, PROCESSED_ANSWER)]

  Si N=1 → FlushAction:
    state_change = compound(coordinator_upstream_eof_change + close_change)
    outputs = [(OUTPUT_QUEUE, DATA×k), (OUTPUT_QUEUE, EOF)]

for entry in outputs: tl_sender(entry.destination).send(entry.body)
commit_done()
ack()
```

### FLUSH_ORDER (thread de control, no-líder)
```
parse_message(msg) → FLUSH_ORDER, client_id, ctrl

handle(f"fo:{client_id}:{ctrl.sender_id}", client_id, sender_id=ctrl.sender_id,
       seq=client_id,   ← único: un FLUSH_ORDER por (cliente, líder)
       bfn):

  bfn lee state.results_for(client_id)   ← antes de que close_change lo borre
  state_change = compound(
      coordinator_cleanup_change  →  coordinator.cleanup_client
      close_change                →  pop processor + contador + cerrar cliente
  )
  outputs = [(OUTPUT_QUEUE, DATA×k), (OUTPUT_QUEUE, EOF),
             (response_queue_líder, FLUSH_ACK)]

for entry in outputs: tl_sender(entry.destination).send(entry.body)
commit_done()
ack()
```

### FLUSH_ORDER (thread de control, líder)
```
parse_message(msg) → FLUSH_ORDER, ...
ID == LEADER_ID → ack() directo (no-op, el líder ignora su propio FLUSH_ORDER)
```

### PROCESSED_ANSWER (thread de respuesta, líder)
```
parse_message(msg) → PROCESSED_ANSWER, client_id, ctrl

← Path directo al coordinator (no vía PersistentStateHandler)
   Limitación: si el líder crashea después del ack pero antes del snapshot,
   el estado acumulado se pierde. RabbitMQ reentrega los mensajes no-ackeados.

with lock: coordinator.process_control_message(PROCESSED_ANSWER, ...) → action

Si action == BroadcastAction(FLUSH_ORDER):
    for qname in action.queue_names:
        tl_sender(qname).send(action.message)
ack()
```

### FLUSH_ACK (thread de respuesta, líder)
```
parse_message(msg) → FLUSH_ACK, client_id, ctrl

← Vía PersistentStateHandler (close_change necesita estar en el WAL)

Predicción read-only (evita doble-acumulación):
  new_ack_count = coordinator.flush_ack_count(client_id) + (0 si ya visto else 1)

handle(f"fa:{client_id}:{ctrl.sender_id}", client_id, sender_id=ctrl.sender_id,
       seq=client_id,   ← único: un FLUSH_ACK por (cliente, no-líder)
       bfn):

  Si new_ack_count >= AGGREGATION_AMOUNT - 1:   ← último FLUSH_ACK
    state_change = compound(
        coordinator_msg_change(FLUSH_ACK)   →  coordinator._on_flush_ack (limpia estado líder)
        close_change                        →  pop processor + contador + cerrar cliente
    )
    outputs = [(OUTPUT_QUEUE, DATA×k), (OUTPUT_QUEUE, EOF)]
  
  Sino:
    state_change = coordinator_msg_change(FLUSH_ACK)
    outputs = []

for entry in outputs: tl_sender(entry.destination).send(entry.body)
commit_done()
ack()
```

---

## Estado (`AggregatorState`)

`AggregatorState` implementa el protocolo `WorkerState` y contiene:

| Campo | Tipo | Descripción |
|---|---|---|
| `_processors_by_client` | `dict[int, AggregatorProcessor]` | Reducción acumulada por cliente. |
| `_data_count_by_client` | `dict[int, int]` | Mensajes DATA recibidos por cliente. |
| `_closed_by_client` | `set[int]` | Clientes ya flusheados (mensajes tardíos ignorados). |
| `_coordinator` | `EofCoordinator` | Estado del protocolo EOF compartido con el worker. |

### Tipos de cambio

| Tipo | Qué hace `apply_change` |
|---|---|
| `"data"` | `processor.accept(payload)` + incrementa `data_count`. |
| `"close"` | Pop processor/contador, agrega a `closed_by_client`. |
| `"coordinator_upstream_eof"` | Llama `coordinator.on_upstream_eof(...)` (idempotente en flush_order: segunda llamada retorna `None`). |
| `"coordinator_msg"` | Llama `coordinator.process_control_message(msg_type, ...)` (FLUSH_ACK acumula estado líder). |
| `"coordinator_cleanup"` | Llama `coordinator.cleanup_client(client_id)` (limpia `_seen_eof`). |
| `"compound"` | Itera `change["changes"]` y aplica cada sub-cambio en orden. |

### Por qué `compound`

`PersistentStateHandler.handle()` escribe **un** registro WAL por mensaje. Cuando
un solo mensaje necesita múltiples mutaciones (p.ej. FLUSH_ORDER necesita
`coordinator_cleanup` + `close`), se las bundlea en un `compound` para mantener
el invariante de un-cambio-por-mensaje.

---

## Durabilidad y recuperación

```
Startup:
  handler.recover()          ← carga snapshot + replay WAL desde el checkpoint
  _republish_pending()       ← re-envía outputs que quedaron en el outbox sin commit

Por mensaje (path happy):
  handle()     →  WAL: InputApplied + apply_change en memoria
  send outputs →  I/O fuera del lock
  commit_done()→  WAL: InputDone + limpia outbox
  ack()        →  RabbitMQ desencola el mensaje

Crash entre handle() y commit_done():
  Mensaje no fue ackeado → RabbitMQ lo reentrega.
  classify(sender, seq) == APPLIED → handler re-publica outputs del outbox.
  commit_done() → ack().

Crash entre commit_done() y ack():
  Mensaje no fue ackeado → RabbitMQ lo reentrega.
  classify(sender, seq) == DONE → handler retorna ACK directo sin reprocesar.
```

### Limitación: PROCESSED_ANSWER no está en el WAL

El estado acumulado del líder (`_leader_processed`, `_leader_responders`) para
PROCESSED_ANSWER no se escribe en el WAL — solo en el snapshot periódico.
Si el líder crashea después de ackear los PROCESSED_ANSWERs pero antes del
próximo snapshot, ese progreso se pierde.

En la práctica esto no causa pérdida de datos: los Sum workers reenvían sus
parciales via RabbitMQ si sus propios mensajes no fueron ackeados. El único
riesgo es un deadlock en el protocolo de coordinación si *todos* los
PROCESSED_ANSWERs ya fueron ackeados. Este caso se resuelve con el mecanismo
de reintentos de `EofCoordinator` (PROCESSED_REQUEST) una vez que el sistema
esté completamente cableado con WAL.

---

## Processors

| Configuración | Clase | Qué reduce |
|---|---|---|
| `Q2` | `Q2AggregatorProcessor` | Máximo monto por banco emisor entre todos los Sum workers. |
| `Q3` | `Q3AggregatorProcessor` | Promedio (suma / conteo) por `payment_format`. |
| `Q5` | `Q5AggregatorProcessor` | Conteo total de transacciones que pasaron el filtro. |

El processor se crea con `create_aggregator_processor(CONFIGURATION)` y vive
dentro de `AggregatorState._processors_by_client[client_id]`. Es picklable
directamente (solo contiene dicts y primitivos), por lo que entra en el snapshot
sin serialización adicional.
