# WAL

El WAL, o Write-Ahead Log, es el log durable que usa cada worker para recordar
qué hizo antes de hacer `ACK` de un mensaje de RabbitMQ. Su objetivo es que un
worker pueda caer y reiniciar sin aplicar dos veces un input y sin perder
salidas que ya había calculado.

La regla central es:

```text
antes de publicar o ACKear, dejar escrito en disco lo necesario para recuperarse
```

El WAL no reemplaza a RabbitMQ ni al snapshot completo del worker. RabbitMQ
reentrega mensajes no ACKeados. `LastState` guarda una foto completa para no
reproducir un log infinito. El WAL cubre la ventana entre el último snapshot y
el punto exacto de caída.

## Componentes

- `record.py`: define el header binario y los tipos de record.
- `writer.py`: escribe records append-only en `wal.current` y hace `fsync`.
- `reader.py`: lee records por LSN, detecta tail truncado y decodifica payloads.
- `wal.py`: fachada pública usada por el resto del sistema.
- `replay.py`: reconstruye el estado derivado del WAL para startup recovery.
- `outbox/outbox_entry.py`: serializa salidas pendientes para republicarlas byte a byte.

Las pruebas unitarias viven en `tests/wal/`.

## Formato

Cada record tiene un header fijo y un payload específico del tipo:

```text
type    1 byte
length  4 bytes, uint32 big-endian
payload length bytes
```

Ejemplo:

```text
01 00 00 00 2a <42 bytes de payload>
```

El `type` identifica la semántica del evento:

| Tipo | Byte | Uso |
|---|---:|---|
| `INPUT_APPLIED` | `0x01` | El input ya modificó estado y generó outbox durable. |
| `INPUT_DONE` | `0x02` | El input terminó; sus salidas ya fueron publicadas/confirmadas. |
| `CLIENT_CLEANUP_STARTED` | `0x03` | Empezó la limpieza durable de un cliente. |
| `EOF_SENT` | `0x04` | Se envió un EOF downstream. |
| `CHECKPOINT` | `0x05` | Un snapshot incorpora el WAL hasta cierto LSN. |

El header length-prefixed permite detectar records incompletos. Si el proceso
muere mientras escribe, puede quedar un último record cortado. El reader lo
descarta porque ese evento no llegó a ser durable de forma confiable.

## LSN

El LSN, Log Sequence Number, es el byte offset donde empieza un record dentro
de `wal.current`.

```text
LSN = offset en bytes desde el inicio del archivo
```

Ejemplo:

```text
record 1 empieza en LSN 0
record 2 empieza en LSN 37
record 3 empieza en LSN 91
```

`LastState` guarda un `wal_checkpoint_record`. Durante recovery, el worker
reproduce solamente los records posteriores:

```python
for record in wal.replay(snapshot.wal_checkpoint_record):
    ...
```

La comparación es estricta: se reproducen records con `lsn > checkpoint_lsn`.

## Protocolo De Procesamiento

Para un input nuevo, el orden esperado es:

1. RabbitMQ entrega el mensaje. No se hace `ACK`.
2. El worker calcula el cambio de estado y las salidas.
3. Se escribe `INPUT_APPLIED` en el WAL y se hace `fsync`.
4. Recién entonces el estado en memoria se marca como `applied` y se guarda el outbox.
5. Se publican las salidas.
6. Cuando las salidas están confirmadas, se escribe `INPUT_DONE` y se hace `fsync`.
7. El input pasa de `applied` a `done`, se limpia su outbox y se hace `ACK`.

El punto delicado es el paso 3. Si el worker cae después de `INPUT_APPLIED`,
al reiniciar sabe que no debe volver a aplicar la lógica de negocio. Debe
reconstruir el outbox y republicar las salidas pendientes.

## Recovery

Al levantar un worker:

1. `LastState` carga el último snapshot válido.
2. El snapshot indica desde qué LSN continuar.
3. `Wal.replay(checkpoint_lsn)` lee los records posteriores al snapshot.
4. `WALReplayer` reconstruye:
   - cambios de estado en orden,
   - inputs `applied`,
   - inputs `done`,
   - outbox pendiente.
5. El `PersistentStateHandler` aplica esos cambios al estado en memoria.
6. Antes de consumir mensajes nuevos, se republica todo outbox pendiente.

El replay no publica mensajes por sí mismo. Solo reconstruye qué quedó
pendiente. Publicar es responsabilidad del loop del worker o del handler que
integra con RabbitMQ.

## Estados De Un Input

Un input puede estar en tres situaciones:

| Estado | Significado | Acción si Rabbit lo reentrega |
|---|---|---|
| `NEW` | No hay record durable de ese input. | Procesar normalmente. |
| `APPLIED` | Hay `INPUT_APPLIED`, pero no `INPUT_DONE`. | No reaplicar lógica; republicar outbox. |
| `DONE` | Hay `INPUT_DONE`. | ACK directo, sin reprocesar. |

Esta separación evita que una caída duplique cambios internos. Si un monto ya
fue sumado al estado, el worker no vuelve a sumarlo; solo reintenta las salidas
que no quedaron cerradas.

## Outbox Durable

Cada `INPUT_APPLIED` contiene las salidas generadas por ese input. Una salida se
guarda como `OutboxEntry`:

```text
output_id
input_id
destination
body
```

`body` es el mensaje serializado completo. En recovery se republica byte a byte,
sin volver a ejecutar lógica de negocio.

Los duplicados downstream son esperables. Por eso cada salida debe tener un
`output_id` determinístico: si el mismo input se reprocesa o se republica,
produce el mismo identificador y el receptor puede deduplicar.

## API Actual

La fachada principal es `Wal`:

```python
from common.fault_tolerance.wal import Wal

wal = Wal("/path/al/state_dir")
lsn = wal.append(record)

for record in wal.replay(from_record=-1):
    ...

next_lsn = wal.current_record_number()
wal.rotate()
wal.close()
```

`append()` hoy acepta:

```text
InputApplied
InputDone
```

Los demás tipos (`EOF_SENT`, `CHECKPOINT`, `CLIENT_CLEANUP_STARTED`) ya tienen
formato y decoders en `WALReader`, pero su integración completa depende de
`LastState`, el estado EOF y la limpieza de clientes.

`WALReplayer` transforma records en una vista útil para recovery:

```python
from common.fault_tolerance.wal import WALReplayer

result = WALReplayer(wal).replay(checkpoint_lsn)

result.state_changes
result.applied_inputs
result.done_inputs
result.pending_outbox
result.outbox_to_republish()
```

## Integración Con PersistentStateHandler

`PersistentStateHandler` es el componente que integra el WAL con el estado en
memoria del worker y con el loop que consume mensajes de RabbitMQ.

Responsabilidades esperadas:

1. Cargar `LastState`.
2. Ejecutar `WALReplayer`.
3. Aplicar `result.state_changes` al `WorkerState`.
4. Reconstruir `Inbox` y `Outbox` desde `result.applied_inputs`, `result.done_inputs` y `result.pending_outbox`.
5. Republicar `result.outbox_to_republish()` antes de consumir mensajes nuevos.
6. Usar `Wal.append(InputApplied)` antes de mutar memoria o publicar.
7. Usar `Wal.append(InputDone)` antes de hacer `ACK` del input.

El WAL no decide si un mensaje es duplicado. Esa clasificación queda en
`Inbox`, pero el WAL provee los eventos durables para reconstruirla.

## Integración Con LastState Y EOF

`LastState` y el estado EOF se coordinan con el WAL durante snapshot y recovery.

Responsabilidades esperadas:

1. Guardar `wal_checkpoint_record` en cada snapshot.
2. Llamar a `Wal.rotate()` solo después de que el snapshot se haya committeado de forma segura.
3. Usar `CHECKPOINT` si se decide dejar una marca explícita en el WAL.
4. Consumir o coordinar `EOF_SENT` para reconstruir estado EOF después de una caída.

El WAL ya sabe decodificar `EOF_SENT`, pero todavía falta definir cómo ese
evento se vuelca al estado persistible del `EofCoordinator`.

## Archivos En Disco

Por worker, la estructura esperada es:

```text
worker_state/
  last_state.current
  last_state.previous
  wal.current
  wal.previous
```

`wal.current` es el log activo. `wal.previous` aparece durante rotación después
de un snapshot. La decisión de cuándo rotar no la toma el WAL solo: debe venir
después de que `LastState` confirmó que el snapshot es durable.

## Garantías

El WAL implementado garantiza:

- append-only con `fsync` por record;
- LSN basado en byte offset;
- replay desde checkpoint;
- descarte de último record truncado;
- serialización durable del outbox;
- reconstrucción de inputs applied/done y salidas pendientes.

No garantiza por sí solo:

- exactly-once end-to-end;
- persistencia si se pierde el volumen completo;
- publisher confirms de RabbitMQ;
- snapshots atómicos;
- deduplicación downstream.

La garantía final aparece cuando se combina con `Inbox`, `Outbox`,
`LastState`, colas personales y `message_id` determinístico.

## Pruebas

Ejecutar desde la raíz del repositorio:

```bash
pytest tests/wal -q
```

La suite cubre:

- formato del header;
- serialización de `OutboxEntry`;
- escritura durable con `WALWriter`;
- lectura y truncamiento con `WALReader`;
- fachada `Wal`;
- reconstrucción con `WALReplayer`.

## Smoke Test Manual

Además de la suite unitaria, existe un script para probar la API del WAL de
forma manual y sin RabbitMQ:

```bash
./scripts/smoke_wal.py
```

El script crea un directorio temporal, escribe records reales en `wal.current`
y muestra paso a paso qué espera recuperar. Cubre los casos principales:

- input completado con `INPUT_APPLIED` + `INPUT_DONE`;
- caída simulada después de `INPUT_APPLIED`, dejando outbox pendiente;
- replay desde checkpoint;
- rotación de `wal.current` a `wal.previous`.

Para inspeccionar los archivos generados:

```bash
./scripts/smoke_wal.py --state-dir /tmp/wal-smoke
```

Para avanzar escenario por escenario:

```bash
./scripts/smoke_wal.py --step --state-dir /tmp/wal-smoke
```

El modo compacto imprime solo los resultados:

```bash
./scripts/smoke_wal.py --quiet
```
