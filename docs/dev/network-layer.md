# Network Layer — Software Design Document

**Branch de implementación**: `feat/fault-tolerance-network` → `feat/worker-state`
**Estado**: implementado y testeado.

---

## 0. Workflow (leer antes de codear)

- **Commits atómicos**: cada commit cubre exactamente una unidad lógica.
- **No asumir nada**: ante cualquier ambigüedad sobre garantías de entrega o orden de mensajes, razonar desde el modelo de fallos. No extrapolar desde sistemas centralizados.
- **Tres preguntas antes de cada decisión**:
  - *Why does this work?* — qué invariante lo sostiene.
  - *What breaks it?* — qué escenario lo viola.
  - *What else could I do?* — qué alternativas existen y por qué se descartaron.
- **Buenas prácticas Python**: type hints en todas las firmas públicas, clases con responsabilidad única, sin magic numbers, PEP 8. Sin comentarios que expliquen *qué* hace el código; solo los que explican *por qué* una decisión no obvia.
- **Testabilidad**: los fixtures de red (publisher confirms, conexión RabbitMQ) deben ser inyectables. Tests no deben tocar RabbitMQ real.

---

## 1. Contexto y responsabilidades

### 1.1 Por qué estas piezas son prerequisito

El módulo `fault_tolerance/` orquesta el ciclo durable de cada mensaje con este orden garantizado:

```
1. INPUT_APPLIED  fsync
2. All outputs    publisher-confirmed
3. INPUT_DONE     fsync
4. RabbitMQ       ack
```

El paso 2 exige que el middleware pueda **confirmar** que RabbitMQ recibió el mensaje. Sin publisher confirms, `PersistentStateHandler` no puede saber si el paso 2 ocurrió antes de pasar al 3.

El paso 3 requiere que el downstream pueda **deduplicar** reentregas. Para eso el mensaje debe llevar `sender_id` y `seq` en el header, que el downstream pasa a `Inbox.classify(client_id, sender_id, seq, kind)`.

**Sin headers de direccionamiento no hay deduplicación posible. Sin publisher confirms la semántica at-least-once no es verificable.**

### 1.2 Responsabilidades

| Responsabilidad | Artefacto |
|---|---|
| Formato binario de mensajes "direccionados" | `message_protocol/internal/protocol.py` |
| Esquema de `sender_id` y `seq` en los mensajes | `message_protocol/internal/protocol.py` |
| Publisher confirms en el path de send | `middleware/middleware_rabbitmq.py` |
| Separación de namespaces DATA vs control en el inbox | `fault_tolerance/inbox/msg_kind.py` |

### 1.3 Interfaces con otros owners

| Owner | Qué espera | Referencia |
|---|---|---|
| **Mateo** (PersistentStateHandler) | `handle(msg_id, client_id, sender_id, seq, payload, fn, kind)` — que `sender_id` y `seq` vengan del header del mensaje de red | `handler/persistent_state_handler.py:87` |
| **Mateo** (worker loop) | `send()` bloquee y confirme antes de retornar, para que `commit_done` sea seguro | `middleware_rabbitmq.py` |
| **Mate** (colas durables) | El header nuevo vive en el body, no en AMQP properties — compatible con cualquier tipo de cola | — |

---

## 2. Modelo de fallos cubierto

**Modelo asumido**: crash-stop. Los procesos mueren abruptamente y se reinician. RabbitMQ no pierde mensajes si las colas son durables y el mensaje tiene `delivery_mode=2`.

| Momento de la caída | Sin headers + sin confirms | Con headers + confirms |
|---|---|---|
| Worker muere entre `basic_publish` y `commit_done` | Downstream no puede deduplicar (no tiene seq) | Downstream deduplica por `(client_id, kind, sender_id, seq)` en el inbox |
| Broker recibe el mensaje pero el worker no sabe si llegó | `commit_done` puede ocurrir en falso | `basic_publish` bloqueó hasta el ack del broker |
| Worker republica el mismo outbox entry dos veces en recovery | El downstream lo procesa dos veces | Lo clasifica DONE y descarta |
| Control message (FLUSH_ACK) llega después de un DATA con mismo sender_id/seq | Inbox lo clasifica DONE y descarta silenciosamente | `MsgKind` separa DATA de control — buckets distintos |

**Fallos no cubiertos**:
- Si el broker falla antes de persistir el mensaje (pese al confirm), este diseño no ayuda. Requiere RabbitMQ Quorum Queues — fuera de scope.
- Si el inbox del downstream no es durable (volumen no montado), los `(kind, sender_id, seq)` conocidos se pierden al reiniciar. Estas piezas son necesarias pero no suficientes.

---

## 3. Feature 1 — Mensajes direccionados (`InternalProtocol`)

### 3.1 Por qué el header original no alcanzaba

El header original:

```
┌─────────┬──────────────────┐
│ type    │ client_id        │
│ 1 byte  │ 16 bytes (uint)  │
└─────────┴──────────────────┘
```

`HEADER_FORMAT = "!B 16s"` → 17 bytes. Sin `sender_id` ni `seq`, el `Inbox.classify()` no puede funcionar.

### 3.2 Header nuevo (addressed packet)

```
┌─────────┬──────────────────┬───────────────┬─────────────────┐
│ type    │ client_id        │ sender_id     │ seq             │
│ 1 byte  │ 16 bytes (uint)  │ 4 bytes (u32) │ 4 bytes (u32)   │
└─────────┴──────────────────┴───────────────┴─────────────────┘
```

`ADDRESSED_HEADER_FORMAT = "!B 16s I I"` → 25 bytes.

- `sender_id` — `int(os.environ["ID"])` del worker emisor. Uint32 big-endian.
- `seq` — contador monótono por `(sender_id, client_id)` asignado por `SenderSequencer`.

**Why does this work?**: el receptor puede clasificar exactamente con `(client_id, sender_id, seq)`. El `SenderSequencer` garantiza que el seq es creciente por `(nodo, cliente)`.

**What breaks it**: si `sender_id` se repite entre workers de tipos distintos enviando al mismo downstream (e.g., `filter_q5_usd_0` y `aggregation_q5_0` ambos con `ID=0` enviando al mismo joiner). Esto **se resuelve con `MsgKind`** — ver sección 5 — sin necesitar coordinación global de IDs.

**What else could I do?**:
- *AMQP message properties*: no toca el formato binario, pero requiere que todos los consumidores extraigan de properties. Más cambios y poco testeable fuera de RabbitMQ real. Descartado.
- *String `output_id` como header*: ya existe en `OutboxEntry.output_id`. El downstream tendría que parsearlo; la deduplicación necesita int. Descartado por fragilidad.
- *Mantener header viejo + nuevo método*: **opción elegida**. `create_packet`/`unpack_packet` intactos; nuevos métodos `create_addressed_packet`/`unpack_addressed_packet` para workers con handler.

### 3.3 Implementación

```python
ADDRESSED_HEADER_FORMAT = "!B 16s I I"
ADDRESSED_HEADER_SIZE = struct.calcsize(ADDRESSED_HEADER_FORMAT)

@classmethod
def create_addressed_packet(cls, msg_type, client_id_bytes, sender_id, seq, payload) -> bytes:
    header = struct.pack(cls.ADDRESSED_HEADER_FORMAT, msg_type, client_id_bytes, sender_id, seq)
    return header + payload

@classmethod
def unpack_addressed_packet(cls, packet) -> tuple[int, int, int, int, bytes]:
    """Returns (msg_type, client_id, sender_id, seq, payload)."""
    header_data = packet[:cls.ADDRESSED_HEADER_SIZE]
    payload = packet[cls.ADDRESSED_HEADER_SIZE:]
    msg_type, client_id_bytes, sender_id, seq = struct.unpack(cls.ADDRESSED_HEADER_FORMAT, header_data)
    client_id = int.from_bytes(client_id_bytes, byteorder="big")
    return msg_type, client_id, sender_id, seq, payload
```

### 3.4 `msg_id` para el handler

Se genera desde los campos del header:

```python
msg_id = f"d:{sender_id}:{client_id}:{seq}"   # DATA
msg_id = f"fo:{client_id}:{ctrl.sender_id}"   # FLUSH_ORDER
msg_id = f"fa:{client_id}:{ctrl.sender_id}"   # FLUSH_ACK
```

Es determinístico: misma reentrega → mismo `msg_id`. El handler usa `msg_id` para indexar el outbox — si el mismo mensaje llega dos veces, el segundo `handle()` encuentra el mismo outbox entry y devuelve `PUBLISH_THEN_COMMIT` sin re-ejecutar el `business_fn`.

---

## 4. Feature 2 — Publisher confirms en el middleware

### 4.1 Por qué el send original no alcanzaba

El `send()` original era fire-and-forget: retornaba sin saber si el broker recibió y persistió el mensaje. Si el broker caía en ese instante, el mensaje se perdía silenciosamente y `commit_done` escribía `INPUT_DONE` al WAL pese a que el output nunca llegó.

### 4.2 Implementación

Variable de entorno `RABBITMQ_PUBLISHER_CONFIRMS` (default `"false"`):

```python
_PUBLISHER_CONFIRMS = os.environ.get("RABBITMQ_PUBLISHER_CONFIRMS", "false").lower() == "true"
```

En `_RabbitMQBase.__init__()`:

```python
self._channel = self._connection.channel()
if _PUBLISHER_CONFIRMS:
    self._channel.confirm_delivery()
```

Con `confirm_delivery()` activo, `basic_publish()` bloquea hasta recibir `basic.ack` o `basic.nack` del broker. Si el broker envía `nack` o el mensaje es unroutable, pika lanza `NackError` / `UnroutableError` — se mapean a `MessageMiddlewareMessageError`.

**Why does this work?**: AMQP publisher confirms (spec §2.3.4). El broker persiste el mensaje en la queue durable antes de enviar el ack.

**What breaks it**:
1. **Throughput**: cada `basic_publish()` espera un round-trip TCP. Aceptable para el TP con pocos workers.
2. **Thread safety**: `BlockingChannel` no es thread-safe. Cada thread debe tener su propia conexión/channel (ya es el caso en `aggregators.py` con `threading.local()`).

---

## 5. Feature 3 — Separación de namespaces con `MsgKind`

### 5.1 El bug de colisión

El `Inbox` clasifica mensajes por `(client_id, sender_id, seq)`. Los mensajes DATA de workers upstream y los mensajes de control (FLUSH_ACK, FLUSH_ORDER) comparten el mismo espacio de `sender_id` (enteros pequeños 0-N).

**Ejemplo concreto**: con `client_id=0` y `filter_q5_usd_1` (ID=1) enviando datos:

```
→ DATA     (client_id=0, sender_id=1, seq=0)  — de filter_q5_usd_1
    inbox registra (0, 1, 0) = DONE

→ FLUSH_ACK (client_id=0, sender_id=1, seq=0)  — de aggregation_q5_1
    inbox busca (0, 1, 0) → ya DONE
    → business_fn nunca se llama → líder nunca recibe el ACK → sistema colgado
```

La colisión ocurre porque `seq=client_id` y `sender_id=ID` — con `client_id=0` y `ID=1`, el primer DATA ya cierra el bucket.

### 5.2 Solución: `MsgKind` como dimensión del inbox

Se agrega `MsgKind(IntEnum)` con tres valores:

```python
class MsgKind(IntEnum):
    DATA            = 0
    CTRL_FLUSH_ORDER = 1
    CTRL_FLUSH_ACK  = 2
```

La clave del inbox pasa de `(client_id, sender_id, seq)` a `(client_id, kind, sender_id, seq)`:

```python
# _done:    dict[client_id, dict[(kind, sender_id), DeduplicationTracker]]
# _applied: dict[client_id, set[(kind, sender_id, seq)]]
```

Cada clase de mensaje vive en su propio bucket. Mismo `sender_id=1`, mismo `seq=0`, pero `kind` diferente → buckets distintos.

**Why does this work?**: la separación es estructural. No depende de que los IDs numéricos estén en rangos distintos — depende de que el tipo del mensaje sea explícito en la clave. Dos mensajes de tipos distintos no pueden colisionar por construcción.

**What breaks it**: si el caller pasa el `kind` incorrecto (e.g., `MsgKind.DATA` para un FLUSH_ACK). El test `test_without_kind_flush_ack_collides_after_one_data_message` demuestra exactamente esto — la colisión sigue ocurriendo si se usa el kind equivocado.

**What else could I do?**:
- *Sender_id con prefijos numéricos grandes* (`0xFFFF_0000 + real_id`): la primera versión del fix. Funciona, pero asume que los IDs reales nunca llegan a esos valores — un invariante implícito y no documentado. Reemplazado por `MsgKind`.
- *Colas separadas para control y datos*: el receptor no necesitaría distinguir por kind porque ya viene de canales distintos. El inbox no tendría el problema. Pero el `PersistentStateHandler` fue diseñado para ser agnóstico al canal — es más limpio que el kind sea parte del mensaje.

### 5.3 Propagación del `kind` a través del stack

El `kind` viaja por todas las capas:

| Capa | Cambio |
|---|---|
| `inbox.py` | `classify()`, `mark_applied()`, `mark_done()` aceptan `kind: MsgKind = DATA` |
| `input_applied.py` / `input_done.py` | Campo `kind: MsgKind = DATA` |
| WAL (writer/reader) | 1 byte entre `sender_id` y `seq` en el formato binario |
| `persistent_state_handler.py` | `handle()` y `commit_done()` aceptan `kind: MsgKind = DATA` |
| `aggregators.py` | Pasa `kind=MsgKind.CTRL_FLUSH_ORDER` o `CTRL_FLUSH_ACK` en rutas de control |

El default `kind=MsgKind.DATA` cubre todos los callers que no son el aggregator — no necesitan cambiar.

### 5.4 `ctx` del `WorkerLoopInstruction`

`handle()` retorna un `WorkerLoopInstruction` con:

```python
ctx = (msg_id, client_id, sender_id, seq, kind)
```

El caller llama `commit_done(*instruction.ctx)` — el handler empaqueta los valores correctos, el caller no necesita recordarlos. Esto evita que el caller pase un `kind` incorrecto a `commit_done` después de haber pasado el correcto a `handle()`.

---

## 6. Ciclo completo con las tres piezas

```
Rabbit entrega M (no ACK)
│
├─ unpack_addressed_packet(M)
│    → (msg_type, client_id, sender_id, seq, payload)
│
├─ msg_id = f"d:{sender_id}:{client_id}:{seq}"
│
├─ handler.handle(msg_id, client_id, sender_id, seq, payload, business_fn,
│                 kind=MsgKind.DATA)
│    → inbox.classify(client_id, sender_id, seq, MsgKind.DATA)
│    → WAL INPUT_APPLIED fsync
│    → apply_change en memoria
│    → outbox.add(entries)
│    → retorna WorkerLoopInstruction(
│          PUBLISH_THEN_COMMIT,
│          outputs=entries,
│          ctx=(msg_id, client_id, sender_id, seq, MsgKind.DATA)
│      )
│
├─ para cada entry en outputs:
│    create_addressed_packet(msg_type, client_id, sender_id_propio, seq_del_entry, body)
│    middleware.send(...)        ← bloquea hasta broker confirm (Feature 2)
│
├─ handler.commit_done(*instruction.ctx)
│    → WAL INPUT_DONE fsync
│    → inbox.mark_done(client_id, sender_id, seq, MsgKind.DATA)
│    → outbox.remove
│
└─ RabbitMQ ack
```

Sin Feature 1: el downstream no puede deduplicar (no tiene sender_id/seq).
Sin Feature 2: `commit_done` puede ocurrir aunque el publish falló silenciosamente.
Sin Feature 3: control messages (FLUSH_ACK, FLUSH_ORDER) colisionan con DATA.
Con las tres: el ciclo es correcto.

---

## 7. Estructura de archivos

```
src/common/
├── fault_tolerance/
│   ├── _encoding.py                 +read_uint8(), +UINT8_FORMAT
│   ├── inbox/
│   │   ├── msg_kind.py              NUEVO — MsgKind(IntEnum)
│   │   ├── inbox.py                 clave (kind, sender_id) en _done y _applied
│   │   └── __init__.py              +MsgKind export
│   ├── handler/
│   │   └── persistent_state_handler.py  handle() y commit_done() aceptan kind
│   └── wal/
│       ├── input_applied.py         +kind: MsgKind = DATA
│       ├── input_done.py            +kind: MsgKind = DATA
│       ├── writer.py                +1 byte kind entre sender_id y seq
│       └── reader.py                +read_uint8() para deserializar kind
├── message_protocol/
│   └── internal/
│       └── protocol.py              +ADDRESSED_HEADER_FORMAT,
│                                    +create_addressed_packet(),
│                                    +unpack_addressed_packet()
└── middleware/
    └── middleware_rabbitmq.py       +_PUBLISHER_CONFIRMS flag,
                                     +confirm_delivery() en __init__,
                                     +NackError/UnroutableError handling
```

---

## 8. Variables de entorno

| Variable | Tipo | Default | Descripción |
|---|---|---|---|
| `RABBITMQ_PUBLISHER_CONFIRMS` | `bool` | `"false"` | Activa confirm mode en todos los channels |
| `RABBITMQ_DURABLE` | `bool` | `"false"` | Ya existe — `delivery_mode=2` para mensajes |

Los workers con `PersistentStateHandler` deben tener ambas en `"true"`.

---

## 9. Testing

### Tests unitarios implementados

- `test_aggregator_control_namespace.py` — 5 tests:
  - Demuestra el bug: con `kind=DATA` en un FLUSH_ACK colisiona y el business_fn no se llama.
  - Verifica que `MsgKind.CTRL_FLUSH_ACK` no colisiona con DATA aunque sender_id y seq sean iguales.
  - Verifica que `MsgKind.CTRL_FLUSH_ORDER` no colisiona con DATA.
  - Verifica idempotencia: reentrega de FLUSH_ACK con kind correcto → clasificado DONE.
  - Verifica que los tres kinds son buckets independientes para el mismo (client, sender, seq).

- `test_writer.py` (actualizado) — verifica round-trip del byte de `kind` en el formato WAL.

### Tests de integración

- `scripts/crash_test_aggregator.py` — Scenario A y B:
  - Levanta stack con 2 aggregators (líder + no-líder).
  - Mata el target durante DATA processing.
  - Reinicia y verifica que el resultado final (9785 rows Q5) coincide con el reference.
  - Los logs `eof_coordinator_flush_ack | sender=1 | acks=1/1` confirman que el FLUSH_ACK llegó al líder después del crash.

---

## 10. Decisiones de diseño

### `sender_id` es uint32, no string

El `Inbox` usa `sender_id: int`. Con string los callers tendrían que parsear. El uint32 soporta hasta 4 mil millones de instancias. La separación por `MsgKind` elimina la necesidad de que los IDs sean globalmente únicos entre tipos de worker.

### `kind` viaja en el WAL, no solo en memoria

Si `kind` no se persiste en el WAL, al recuperar tras un crash el replay llama `inbox.classify()` con `kind=DATA` (default) para todos los records — incluyendo los que eran FLUSH_ACK o FLUSH_ORDER. Esto los re-clasificaría en el bucket DATA, rompiendo la deduplicación. Por eso `kind` es un campo del WAL record.

### `commit_done(*instruction.ctx)` en lugar de pasar los parámetros manualmente

`handle()` empaqueta `(msg_id, client_id, sender_id, seq, kind)` en `instruction.ctx`. El caller usa `*ctx` para desempacar. Esto garantiza que `commit_done` usa exactamente los mismos valores que `handle()` usó para clasificar — el caller no puede pasar un `kind` incorrecto por error de copia.

### `create_packet`/`unpack_packet` no se tocaron

Cambiar el formato existente rompe todos los workers de golpe. Los workers que Pana migra al handler usan los métodos `_addressed_*`; los demás siguen con los originales hasta que el equipo los migre.

---

## 11. Limitaciones conocidas

| Situación | Comportamiento |
|---|---|
| Broker baja después del confirm pero antes de que el consumer reciba | El mensaje está en la queue durable; el consumer lo recibirá al reconectar |
| Thread llama `send()` concurrentemente en la misma instancia | Race condition en el channel; cada thread debe tener su propia instancia (ya es el caso con `threading.local()` en el aggregator) |
| WAL format cambió (1 byte extra por record) | WAL files escritos antes del cambio son incompatibles. En producción requeriría migración; en el TP se teardownea el volumen al desplegar |
| Workers no migrados al handler aún usan `create_packet` sin addressed fields | El downstream no puede deduplicar esos mensajes. Limitación temporal hasta completar la migración |
