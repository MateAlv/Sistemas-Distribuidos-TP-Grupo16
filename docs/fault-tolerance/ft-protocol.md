# Protocolo de tolerancia a fallos

El sistema combina dos mecanismos:

- **Estado durable por worker:** cada worker persiste su estado de negocio, inbox,
  outbox y secuenciador antes de confirmar mensajes de RabbitMQ.
- **Recuperación de containers:** los monitores detectan procesos caídos por
  heartbeat, eligen un líder y el líder reinicia el container detenido.

La recuperación de containers vuelve a ejecutar el proceso. La recuperación de
estado garantiza que ese proceso no pierda ni aplique dos veces un input.

## Procesamiento durable

Cada input pasa por tres etapas durables:

1. **Apply:** el worker calcula el cambio de estado y las salidas, escribe
   `INPUT_APPLIED` en el WAL y recién entonces muta memoria.
2. **Publish:** el worker publica las salidas guardadas en outbox y espera
   publisher confirms.
3. **Commit:** el worker escribe `INPUT_DONE`, elimina del outbox las salidas de
   ese input y hace `ACK` del mensaje original.

Si el proceso cae antes del `ACK`, RabbitMQ reentrega el input. La inbox durable
clasifica la redelivery como:

| Estado | Significado | Acción |
|---|---|---|
| `NEW` | No hay registro durable. | Procesar normalmente. |
| `APPLIED` | Estado/outbox ya fueron persistidos, pero falta commit. | Republicar outbox sin reaplicar negocio. |
| `DONE` | El input terminó. | Hacer `ACK` sin reprocesar. |

## Recovery de un worker

Al iniciar, `PersistentStateHandler.recover()`:

1. Carga el último snapshot válido (`LastState`).
2. Restaura `WorkerState`, inbox, outbox y `SenderSequencer`.
3. Reproduce los records de `wal.current` posteriores al checkpoint.
4. Aplica `INPUT_APPLIED` e `INPUT_DONE` de forma idempotente.
5. Deja disponible `outbox_to_republish()` para reenviar salidas pendientes
   antes de consumir mensajes nuevos.

Los snapshots evitan que el WAL crezca indefinidamente. El handler solo rota el
WAL después de que el snapshot fue escrito de forma durable.

## Mensajes addressed

Las aristas durables usan paquetes addressed con:

```text
msg_type | client_id | sender_id | seq | payload
```

El consumidor deduplica por `(client_id, kind, sender_id, seq)`. `kind` separa
DATA de mensajes de control para que un `FLUSH_ACK` no choque con un DATA que
casualmente tenga el mismo sender y secuencia.

`SenderSequencer` asigna secuencias densas por destino/shard para mantener
acotada la estructura de deduplicación.

## EOF y cierre por cliente

Los workers con varias réplicas coordinan EOF mediante `EofCoordinator`.
Dependiendo de la topología, el modo es:

- **broadcast:** las réplicas consumen de una cola compartida; la que recibe el
  EOF se vuelve líder dinámico para ese cliente.
- **flush_order:** cada réplica tiene su shard; un líder fijo junta reportes y
  ordena el flush.

El estado del coordinador vive dentro del `WorkerState`, por lo que se guarda en
snapshots y se reconstruye durante recovery.

## Monitor y reinicio

Cada worker emite heartbeats UDP a todos los monitores. Las réplicas de monitor
usan elección Bully; solo el líder ejecuta recuperación. Si un nodo supera
`check_interval * max_missed` sin heartbeat, el líder ejecuta `docker start`
sobre el container detenido.

Cada monitor persiste su epoch en `data/monitor/monitor_<id>/epoch.json` para
evitar retrocesos de liderazgo tras reinicios.

## Garantías

Con volúmenes persistentes y RabbitMQ disponible, el protocolo garantiza:

- ningún mensaje confirmado se pierde;
- un input redelivered no se aplica dos veces;
- las salidas pendientes se republican después de una caída;
- los duplicados downstream son absorbidos por deduplicación durable;
- los containers detenidos vuelven a levantarse cuando el monitor líder los
  detecta.

No cubre pérdida completa del volumen de estado ni particiones de red
prolongadas entre monitores y workers.

Para más detalle del WAL y sus archivos, ver [wal.md](wal.md). Para ver el contrato
del handler y las estructuras persistidas, ver
[src/common/fault_tolerance/README.md](../../src/common/fault_tolerance/README.md).
