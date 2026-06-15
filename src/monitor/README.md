# Monitor

El módulo de monitor detecta procesos caídos, elige una única réplica líder y
recupera containers detenidos. Cada proceso monitoreado envía heartbeats UDP a
todas las réplicas. Los monitores coordinan el liderazgo mediante el algoritmo
Bully y el líder recupera los containers con `docker start`.

## Componentes

- `main.py`: carga la configuración y construye el monitor.
- `monitor.py`: ejecuta el ciclo de detección, liderazgo y recuperación.
- `heartbeat/heartbeat_receiver.py`: recibe y registra los heartbeats UDP.
- `election/election_handler.py`: implementa la elección Bully por TCP.
- `election/messages.py`: define el protocolo binario de elección.
- `recovery.py`: inicia containers mediante el socket de Docker.

Los containers de monitor montan `/var/run/docker.sock` para acceder al daemon
del host. Solamente la réplica líder evalúa y recupera workers. Las réplicas
secundarias vigilan al líder y comienzan una elección si deja de responder.

## Configuración

El bloque `monitor` de `config/main-config.yaml` controla las réplicas:

```yaml
monitor:
  enabled: true
  count: 3
  port: 9000
  election_port: 9001
  check_interval: 3
  max_missed: 3
  election_timeout: 5
  coordinator_timeout: 10
  startup_grace_period: 30
```

| Campo | Uso |
|---|---|
| `enabled` | Habilita la generación de los servicios de monitor. |
| `count` | Cantidad de réplicas. Los IDs van de `1` a `count`. |
| `port` | Puerto UDP usado para recibir heartbeats. |
| `election_port` | Puerto TCP usado por el protocolo de elección. |
| `check_interval` | Frecuencia con la que el monitor revisa el estado. |
| `max_missed` | Cantidad de intervalos tolerados sin heartbeat. |
| `election_timeout` | Timeout para mensajes entre réplicas. |
| `coordinator_timeout` | Espera máxima por el anuncio de un nuevo líder. |
| `startup_grace_period` | Tiempo inicial en que un nodo nunca visto no se considera caído. |

Un nodo se considera caído cuando pasa más de:

```text
check_interval * max_missed
```

Con la configuración de ejemplo, el timeout de detección es de nueve segundos.
Durante los primeros 30 segundos, un nodo que todavía no envió ningún
heartbeat no se recupera. Esto evita falsos positivos mientras los workers
esperan que RabbitMQ y sus dependencias estén disponibles. Una vez recibido el
primer heartbeat, se usa siempre el timeout normal de nueve segundos.

Para deshabilitar el módulo:

```yaml
monitor:
  enabled: false
```

## Elección y recuperación

Al comenzar, las réplicas realizan una elección Bully. La réplica activa con
mayor ID gana y anuncia un mensaje `COORDINATOR`.

El ciclo de cada réplica funciona así:

1. Si es líder, revisa los heartbeats y recupera los nodos caídos.
2. Si todavía no conoce un líder, espera el anuncio o inicia una elección.
3. Si es secundaria y el líder sigue activo, no recupera workers.
4. Si el líder deja de responder, inicia una nueva elección.
5. Si vuelve una réplica con ID mayor, puede reclamar nuevamente el liderazgo.

La recuperación ejecuta `docker start` sobre el nombre del servicio detectado.
Los eventos principales son:

```text
monitor_election_started
monitor_election_won
monitor_election_cancelled
monitor_coordinator_accepted
monitor_node_failed
monitor_recovery_start
monitor_recovery_success
monitor_recovery_failed
```

## Preparar pruebas manuales

Todos los comandos deben ejecutarse desde la raíz del repositorio.

Los targets `monitor-*` trabajan con `docker-compose.yaml`. Los targets
`make test-q1`, `make test-q2`, `make test-q3` y `make test-q5` generan otro
proyecto en `docker-compose.test.yaml` y no habilitan monitores.

Habilitar el monitor y Chaos Monkey en `config/main-config.yaml`:

```yaml
settings:
  chaos:
    enabled: true
    interval: 3600

monitor:
  enabled: true
  count: 3
```

El intervalo alto evita que Chaos Monkey mate containers automáticamente
durante la prueba.

Limpiar y regenerar el compose:

```bash
make down
make config
docker compose -f docker-compose.yaml config --services
```

Después de cambiar `startup_grace_period` es necesario reconstruir y reiniciar
los containers de monitor para que reciban la nueva variable de entorno.

La lista debe incluir `monitor_1`, `monitor_2`, `monitor_3`, algún worker como
`filter_usd_0` y `chaos_monkey`.

Levantar el sistema:

```bash
make up
```

`make up` deja los logs en seguimiento. Conviene mantener esa terminal abierta
y usar otra para las pruebas. Al presionar `Ctrl+C`, `pretty_logs.py` puede
mostrar `KeyboardInterrupt` y Make puede terminar con `Error 130`. Eso solo
indica que se interrumpió el seguimiento de logs: los containers se levantaron
con `--detach` y continúan ejecutándose.

En otra terminal:

```bash
docker compose -f docker-compose.yaml ps
make monitor-status
```

Los tres monitores deben estar en estado `running`. Normalmente `monitor_3`,
la réplica de mayor ID, será el líder.

## Logs

Seguir solamente los logs de monitor:

```bash
make monitor-logs
```

El comando queda abierto hasta presionar `Ctrl+C`. Para consultar los últimos
eventos sin seguimiento:

```bash
docker compose -f docker-compose.yaml logs --tail 100 \
  monitor_1 monitor_2 monitor_3
```

Para ver el estado y los eventos de los últimos cinco minutos:

```bash
make monitor-status
```

## RabbitMQ Management

Abrir la interfaz:

```bash
make rabbit-screen
```

También puede abrirse manualmente:

```text
http://localhost:15672/#/queues
usuario: guest
clave: guest
```

Los heartbeats usan UDP y no aparecen en RabbitMQ. La UI sirve para comprobar
que el procesamiento vuelve a avanzar después de recuperar un worker.

| Métrica | Qué comprobar |
|---|---|
| `Ready` | Que una cola atrasada vuelva a bajar al recuperar el worker. |
| `Unacked` | Que no quede permanentemente alto después de la caída. |
| `incoming` | Que los productores continúen publicando. |
| `deliver / get` y `ack` | Que el consumo y las confirmaciones se reanuden. |

## Caso 1: funcionamiento normal

Dejar el sistema activo durante más de `check_interval * max_missed`:

```bash
make monitor-status
```

No deben aparecer eventos `monitor_node_failed` para containers que continúen
corriendo.

## Caso 2: recuperación de un worker

Confirmar que el servicio existe y está activo:

```bash
docker compose -f docker-compose.yaml ps filter_usd_0
```

Prueba automatizada:

```bash
make monitor-test-recovery CONTAINER=filter_usd_0
```

El target detiene el worker y espera que el líder lo inicie nuevamente. La
salida esperada incluye:

```text
monitor_node_failed
monitor_recovery_start
monitor_recovery_success
PASS: filter_usd_0 was restarted by the monitor
```

Prueba completamente manual:

```bash
docker compose -f docker-compose.yaml stop -t 0 filter_usd_0
make monitor-logs
```

Después del timeout de detección:

```bash
docker compose -f docker-compose.yaml ps filter_usd_0
```

El worker debe volver a `running`. En RabbitMQ puede observarse que su cola
acumula mensajes mientras está detenido y vuelve a drenar cuando se recupera.

El timeout de la prueba debe superar el tiempo de detección:

```bash
MONITOR_TEST_TIMEOUT=45 make monitor-test-recovery \
  CONTAINER=filter_usd_0
```

Si aparece `Unknown compose service`, regenerar el compose y consultar los
nombres válidos:

```bash
make config
docker compose -f docker-compose.yaml config --services
```

## Caso 3: caída del líder

Confirmar que las réplicas y Chaos Monkey estén activos:

```bash
docker compose -f docker-compose.yaml ps \
  monitor_1 monitor_2 monitor_3 chaos_monkey
```

Prueba automatizada:

```bash
make monitor-test-election
```

El target mata al monitor de mayor ID. Se espera que `monitor_2` gane
temporalmente la elección, recupere `monitor_3` y que el cluster vuelva a
converger con `monitor_3` como líder. La prueba también exige que el epoch de
`monitor_3` sea mayor que el epoch temporal de `monitor_2`, y que todos los
followers acepten exactamente ese nuevo epoch.

Prueba paso a paso:

```bash
make chaos-kill CONTAINER=monitor_3
# Forma abreviada equivalente:
make chaos-kill monitor_3
make monitor-logs
```

Los logs deben mostrar eventos equivalentes a:

```text
monitor_election_started
monitor_election_won | monitor_id=2 | epoch=2
monitor_node_failed | node_id=monitor_3
monitor_recovery_start | node_id=monitor_3
monitor_recovery_success | node_id=monitor_3
monitor_election_won | monitor_id=3 | epoch=3
monitor_coordinator_accepted ... leader_id=3 | epoch=3 | announced_epoch=3
```

Confirmar la convergencia:

```bash
docker compose -f docker-compose.yaml ps \
  monitor_1 monitor_2 monitor_3
```

El límite de espera puede aumentarse:

```bash
MONITOR_FAILOVER_TIMEOUT=60 make monitor-test-election
```

## Caso 4: caída de un monitor secundario

Con `monitor_3` como líder:

```bash
make chaos-kill CONTAINER=monitor_1
make monitor-logs
```

El líder debe detectar y recuperar `monitor_1`. No debe cambiar el líder porque
`monitor_3` continúa activo:

```bash
docker compose -f docker-compose.yaml ps monitor_1
```

## Caso 5: una sola recuperación

Detener un worker y observar las tres réplicas:

```bash
docker compose -f docker-compose.yaml stop -t 0 filter_usd_0
docker compose -f docker-compose.yaml logs -f \
  monitor_1 monitor_2 monitor_3
```

Solamente el líder debe evaluar la caída y producir un único par
`monitor_recovery_start` y `monitor_recovery_success`. Las réplicas secundarias
se limitan a vigilar que el líder continúe activo.

## Tests unitarios

Ejecutar todos los tests del módulo:

```bash
pytest tests/monitor
```

Los tests cubren configuración, recepción de heartbeats, mensajes binarios,
elección, coordinación, detección de fallas y recuperación.

## Cerrar las pruebas

```bash
make down
```
