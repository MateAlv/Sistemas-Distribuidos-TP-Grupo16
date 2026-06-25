# Money Laundering Analysis - Grupo 16

Sistema distribuido para procesar datasets de transacciones bancarias y detectar patrones asociados a lavado de dinero. El sistema usa Python, Docker Compose y RabbitMQ como MOM, con workers escalables, tolerancia a fallos (WAL + snapshots + monitores) y soporte multicliente.

Enunciado: [docs/FIUBA-Entregas/enunciado/enunciado.md](docs/FIUBA-Entregas/enunciado/enunciado.md).

## Requisitos

- Docker y Docker Compose.
- Python 3.12 o compatible, con las dependencias de `requirements.txt`.
- Los datasets montados bajo `data/datasets/<DATASET>/`.

La estructura esperada para un dataset es:

```text
data/datasets/LI-Mini/LI-Mini_Trans.csv
data/datasets/LI-Mini/LI-Mini_accounts.csv
```

Datasets disponibles en el repo, de menor a mayor tamaño:

| Dataset | Tamaño | Uso típico |
|---|---|---|
| `LI-Mini` | chico | smoke test rápido, iteración local |
| `LI-Small` | mediano | validación funcional completa |
| `HI-Medium` | grande (~3 GB de transacciones) | corridas de performance y tolerancia a fallos |

Para multiclient no hace falta replicar físicamente el dataset: la configuración genera varios clientes apuntando al dataset base, cada uno con un `client_id` distinto.

## Probar todo con `make test`

`make test` es el comando principal para correr **las cinco queries de punta a punta** y validar el resultado. Está manejado por `config/test-config.yaml` y por el script `scripts/run_full_test.py`.

```bash
make test
```

### Qué hace, paso a paso

1. **Genera `docker-compose.test.yaml`** a partir de `config/test-config.yaml` (cantidad de workers, clientes, dataset, monitores y monkey de fallos).
2. **Precomputa los resultados esperados** del dataset si todavía no existen (referencia contra la que se valida).
3. **Levanta el stack de test** (`docker compose up --build`) en el proyecto `distribuidos-test`.
4. **Captura los logs formateados** de toda la corrida en `data/logs/test-<timestamp>.log`, con un puntero estable en `data/logs/latest.log`.
5. **Espera a que terminen todos los clientes** (hasta su exit o `TEST_CLIENT_WAIT_TIMEOUT`).
6. **Valida la salida de cada cliente** contra la referencia, query por query (contemplando clientes abortados, ver abajo).
7. **Imprime los resúmenes** de la corrida: elección de monitor / failovers, kills del monkey, cleanup por ABORT y un resumen final por query.
8. **Baja y limpia** los containers, salvo que se use `KEEP_CONTAINERS=1`.

Al terminar, un test exitoso muestra cada query con `✓ matches reference` y un resumen final en verde.

### Elegir el dataset

El dataset a usar se define en `client_accounts` dentro de `config/test-config.yaml`. Para forzar otro dataset en una corrida puntual sin editar el YAML:

```bash
TEST_DATASET=LI-Small make test
```

### Tolerancia a fallos durante el test (monkey)

La inyección de fallos se configura en `settings.monkey` dentro de `config/test-config.yaml`. Cuando está habilitada, durante la corrida se matan containers de workers (y opcionalmente se fuerza un failover de monitor o una desconexión de cliente), y el sistema debe recuperarse y **producir igualmente el resultado correcto**.

Claves principales de `settings.monkey`:

| Clave | Significado |
|---|---|
| `enabled` | Activa/desactiva la inyección de fallos. |
| `max_kills` | Máximo de kills (`0` = ilimitado hasta que termina la query). |
| `interval_min` / `interval_max` | Rango de segundos entre kills. |
| `kill_monitor_leader_first` | El primer kill apunta al líder de monitores para forzar un failover. |
| `client_disconnect_abort` | El primer kill desconecta un cliente a mitad de upload para ejercitar el flujo de ABORT. |
| `targets.<worker>` | Qué tipos de worker pueden ser asesinados. |

Cuando un cliente se desconecta a mitad de upload, el gateway difunde un **ABORT**: todos los workers descartan el estado de ese cliente y los clientes que sí terminan se validan normalmente. El resumen de ABORT al final del test indica qué clientes fueron abortados y se excluyen de la validación contra la referencia.

### Salidas de una corrida

- **Resultados**: `data/output/results_q<n>_<client_id>.csv`
- **Log completo**: `data/logs/test-<timestamp>.log` (y `data/logs/latest.log`)

```text
data/output/results_q1_<client_id>.csv
data/output/results_q2_<client_id>.csv
data/output/results_q3_<client_id>.csv
data/output/results_q4_<client_id>.csv
data/output/results_q5_<client_id>.csv
```

## Otros comandos

Generar `docker-compose.yaml` desde la configuración principal:

```bash
make config
```

Levantar el sistema completo definido en `config/main-config.yaml` (posible entorno de "producción", no recomendado para probar):

```bash
make up
```

Frenar y limpiar containers/redes del compose principal y de tests:

```bash
make down
```

Forzar una limpieza más agresiva:

```bash
make hard-down
```

Limpiar outputs y datasets temporales por cliente:

```bash
make clean-state
```

Ver logs formateados:

```bash
make logs
```

Ejecutar tests unitarios dentro de Docker (corre toda la suite):

```bash
make test-unit
```

Para correr un subconjunto, usar `make run-tests` con `PYTEST_ARGS` (por default
corre toda la suite, igual que `make test-unit`):

```bash
make run-tests PYTEST_ARGS="tests/workers/test_joiner.py -k abort -v"
```

Ambos construyen la misma imagen desde `Dockerfile.test`; la única diferencia es
que `run-tests` permite elegir qué tests correr vía `PYTEST_ARGS`.

## Variables útiles

Variables de dataset:

| Variable | Uso |
|---|---|
| `TEST_DATASET` | Fuerza el dataset para `make test` (override de `config/test-config.yaml`). |

Variables de concurrencia y escalado:

| Variable | Uso |
|---|---|
| `CLIENTS` | Cantidad de clientes simultáneos. |
| `USD_WORKERS` | Réplicas de `filter_usd`. |
| `Q2_SUM_WORKERS` | Réplicas de `sum_q2`. |
| `Q3_BARRIER_WORKERS` | Réplicas de `q3_barrier`, particionadas por `client_id`. |
| `Q5_FORMAT_WORKERS` | Réplicas de `filter_q5_format`. |
| `Q5_USD_WORKERS` | Réplicas de `filter_q5_usd`. |
| `Q4_FILTER_WORKERS` | Réplicas de `q4_filter`. |
| `Q4_SUM_WORKERS` | Réplicas de `q4_sum`. |
| `Q4_JOINER_WORKERS` | Réplicas de `q4_joiner`. |
| `Q4_AGGREGATOR_WORKERS` | Réplicas de `q4_aggregator`. |
| `Q4_DEDUPER_WORKERS` | Réplicas de `q4_deduper`. |
| `PREFETCH_COUNT` | Prefetch de RabbitMQ en workers que lo soportan. |

Variables de ejecución:

| Variable | Uso |
|---|---|
| `KEEP_CONTAINERS=1` | Mantiene los containers vivos al terminar el test. |
| `TEST_CLIENT_WAIT_TIMEOUT` | Timeout para esperar a los clientes. Ej: `3600s`. |
| `TEST_SMOKE_DEADLINE_SECONDS` | Deadline de smoke checks en `make test`. |
| `TEST_LOG_KEEP` | Cuántas corridas conservar en `data/logs/`. Default: `5`. |
| `TEST_LOG_MAX_MB` | Tope de tamaño del archivo de logs en `data/logs/`. Default: `1024`. |
| `LOG_COLOR=never` | Desactiva color en logs formateados. |
| `LOG_ARGS` | Argumentos extra para `docker compose logs`. |
| `FLOW_LOG_EVERY_MESSAGES` | Cada cuántos mensajes DATA loguear progreso por publisher/consumer. Default: `100`. |
| `FLOW_LOG_EVERY_BYTES` | Cada cuántos bytes de RabbitMQ loguear progreso por publisher/consumer. Default: `8388608`. |
| `WORKER_LOG_EVERY_MESSAGES` | Cada cuántos batches DATA loguear progreso semántico dentro de cada worker. Default: `100`. |
| `FLOW_LOG_ENABLED=0` | Desactiva los logs de flujo generados por el middleware. |
| `CHUNK_LOG_EVERY` | Cada cuántos chunks cliente/gateway loguear progreso. Default: `100`. |
| `RESULT_LOG_EVERY` | Cada cuántas líneas de resultado gateway->cliente loguear progreso. Default: `100`. |

## Crash tests de tolerancia a fallos

Además del monkey integrado en `make test`, hay scripts dedicados que matan un worker puntual con SIGKILL en el momento justo, lo reinician y validan que el resultado siga siendo correcto:

| Script | Qué prueba |
|---|---|
| `scripts/crash_test_aggregator.py` | Recuperación del aggregator (`q2`, `q3`, `q5`). |
| `scripts/crash_test_joiner.py` | Recuperación del joiner. |
| `scripts/crash_test_filter_q5_usd.py` | Recuperación del filtro Q5/USD. |
| `scripts/crash_test_q2_bank_name_joiner.py` | Recuperación del joiner de nombre de banco (Q2). |
| `scripts/crash_test_q3_barrier.py` | Recuperación de la barrera de Q3. |
| `scripts/crash_test_abort.py` | Propagación de ABORT al desconectarse un cliente a mitad de upload. |

`scripts/ft_monitor.py` permite monitorear en tiempo real el estado de los containers de Docker durante una corrida.

## Monitor y recuperación

El sistema incluye réplicas de monitor con heartbeats UDP, elección Bully y
recuperación de containers mediante Docker. La explicación de la arquitectura,
la configuración y todos los casos de prueba manuales está en
[src/monitor/README.md](src/monitor/README.md).

## RabbitMQ

Abrir la pantalla de colas:

```bash
make rabbit-screen
```

Credenciales:

```text
guest / guest
```

URL por default:

```text
http://localhost:15672/#/queues
```

Para inspeccionar desde consola durante un test con `KEEP_CONTAINERS=1`:

```bash
docker compose -p distribuidos-test -f docker-compose.test.yaml logs -f rabbitmq
docker compose -p distribuidos-test -f docker-compose.test.yaml logs -f gateway
docker compose -p distribuidos-test -f docker-compose.test.yaml logs -f q3_barrier_0
```

Al mirar RabbitMQ, las columnas más útiles son:

| Métrica | Lectura |
|---|---|
| `Ready` | Mensajes encolados esperando consumidor. Si crece sostenidamente, hay bottleneck aguas abajo. |
| `Unacked` | Mensajes entregados pero todavía no confirmados. Si queda alto, revisar prefetch, CPU o errores del consumer. |
| `incoming` | Tasa de publicación hacia la cola. |
| `deliver / get` | Tasa de consumo desde la cola. |
| `ack` | Tasa de confirmación. Debe acompañar a `deliver / get` en flujos sanos. |

## Performance

Ver consumo de CPU y memoria por container:

```bash
make stats
```

O directamente:

```bash
docker stats
```

Patrones prácticos para diagnosticar cuellos:

- Si `Ready` sube y `deliver / get` es bajo, el consumer de esa cola no da abasto.
- Si `incoming` es muy alto por mensajes chicos, conviene revisar batching del hop anterior.
- Si RabbitMQ marca alarma de memoria, bajar fan-out de mensajes individuales, aumentar batching o reducir concurrencia.
- Si hay mucho `Unacked`, revisar `PREFETCH_COUNT`, tiempo de procesamiento y si el worker está bloqueado publicando hacia otra cola.
- Si el cliente no termina aunque una query produjo output, revisar EOFs pendientes en gateway para todas las queries activas.

Para una corrida de performance conviene usar un dataset grande (`HI-Medium`) con los containers vivos:

```bash
TEST_DATASET=HI-Medium KEEP_CONTAINERS=1 make test
```

## Troubleshooting

Si un test queda con containers vivos:

```bash
docker compose -p distribuidos-test -f docker-compose.test.yaml down --volumes --remove-orphans
```

Si hay containers o redes colgadas:

```bash
make hard-down
```

Si se quiere arrancar sin outputs anteriores:

```bash
make clean-state
```

Si Docker muestra resultados viejos o servicios inesperados, regenerar compose y levantar de cero:

```bash
make down
make config
make up
```
