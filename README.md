# Money Laundering Analysis - Grupo 16

Sistema distribuido para procesar datasets de transacciones bancarias y detectar patrones asociados a lavado de dinero. El sistema usa Python, Docker Compose y RabbitMQ como MOM, con workers escalables y soporte multicliente.

Enunciado: [docs/enunciado.md](docs/enunciado.md).

## Requisitos

- Docker y Docker Compose.
- Python 3.12 o compatible.
- Los datasets montados bajo `data/datasets/<DATASET>/`.
- Opcional: entorno virtual en `venv/`. El Makefile usa `venv/bin/python` si existe, o `python3` en caso contrario.

La estructura esperada para un dataset es:

```text
data/datasets/LI-Mini/LI-Mini_Trans.csv
data/datasets/LI-Mini/LI-Mini_accounts.csv
```

Para multicliente no hace falta replicar fisicamente el dataset: los presets generan varios clientes apuntando al dataset base, cada uno con un `client_id` distinto.

## Comandos principales

Generar `docker-compose.yaml` desde la configuración principal:

```bash
make config
```

Levantar el sistema completo definido en `config/main-config.yaml`:

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

Ejecutar tests unitarios dentro de Docker:

```bash
make test-unit
```

## Correr queries individuales

Cada target genera un `docker-compose.test.yaml` con un preset mínimo para esa query, levanta los containers, espera a los clientes y valida los archivos de salida contra el dataset indicado.

Q1:

```bash
make test-q1
Q1_DATASET=LI-Small CLIENTS=3 USD_WORKERS=4 PREFETCH_COUNT=50 make test-q1
```

Q2:

```bash
make test-q2
Q2_DATASET=LI-Small CLIENTS=3 USD_WORKERS=4 Q2_SUM_WORKERS=2 PREFETCH_COUNT=50 make test-q2
```

Q3:

```bash
make test-q3
Q3_DATASET=LI-Small CLIENTS=3 USD_WORKERS=4 Q3_BARRIER_WORKERS=3 PREFETCH_COUNT=50 make test-q3
```

Q4:

Q4 no tiene un target específico `test-q4`; se ejecuta dentro del flujo general con `make test`, `make up` o escenarios. Para enfocarse en Q4, escalar los workers scatter-gather y dejar containers vivos para mirar RabbitMQ:

```bash
KEEP_CONTAINERS=1 TEST_DATASET=LI-Mini CLIENTS=3 \
USD_WORKERS=4 SG_MAPPER_WORKERS=2 SG_LINKER_WORKERS=2 SG_DETECTOR_WORKERS=2 \
PREFETCH_COUNT=50 make test
```

Q5:

```bash
make test-q5
Q5_DATASET=LI-Small CLIENTS=5 USD_WORKERS=4 Q5_FORMAT_WORKERS=4 Q5_USD_WORKERS=4 PREFETCH_COUNT=50 make test-q5
```

Si se quiere dejar el entorno vivo para inspeccionar RabbitMQ o logs después del test:

```bash
KEEP_CONTAINERS=1 CLIENTS=3 Q5_DATASET=LI-Small USD_WORKERS=4 Q5_FORMAT_WORKERS=4 Q5_USD_WORKERS=4 PREFETCH_COUNT=50 make test-q5
```

Para bajarlo luego:

```bash
docker compose -p distribuidos-test -f docker-compose.test.yaml down --volumes --remove-orphans
```

o tambien

```bash
make down
```

## Correr todas las queries

El target general `make test` usa `config/test-config.yaml` y valida el flujo completo por smoke checks de logs y finalización de clientes.

```bash
make test
```

Con dataset y escalado:

```bash
TEST_DATASET=LI-Mini CLIENTS=3 USD_WORKERS=4 Q5_FORMAT_WORKERS=4 Q5_USD_WORKERS=4 PREFETCH_COUNT=50 make test
```

Ejemplo más exigente:

```bash
CLIENTS=3 USD_WORKERS=4 Q5_FORMAT_WORKERS=4 Q5_USD_WORKERS=4 \
SG_MAPPER_WORKERS=2 SG_LINKER_WORKERS=2 SG_DETECTOR_WORKERS=2 \
Q3_BARRIER_WORKERS=3 PREFETCH_COUNT=50 \
TEST_CLIENT_WAIT_TIMEOUT=3600s TEST_SMOKE_DEADLINE_SECONDS=3600 \
make test
```

Por default, `TEST_DATASET` depende de la configuración de `config/test-config.yaml`. Para forzarlo:

```bash
TEST_DATASET=LI-Small make test
```

## Variables útiles

Variables de dataset:

| Variable | Uso |
|---|---|
| `TEST_DATASET` | Dataset para `make test`. |
| `Q1_DATASET` | Dataset para `make test-q1`. |
| `Q2_DATASET` | Dataset para `make test-q2`. |
| `Q3_DATASET` | Dataset para `make test-q3`. |
| `Q5_DATASET` | Dataset para `make test-q5`. |

Variables de concurrencia y escalado:

| Variable | Uso |
|---|---|
| `CLIENTS` | Cantidad de clientes simultáneos. |
| `USD_WORKERS` | Réplicas de `filter_usd`. |
| `Q2_SUM_WORKERS` | Réplicas de `sum_q2`. |
| `Q3_BARRIER_WORKERS` | Réplicas de `q3_barrier`, particionadas por `client_id`. |
| `Q5_FORMAT_WORKERS` | Réplicas de `filter_q5_format`. |
| `Q5_USD_WORKERS` | Réplicas de `filter_q5_usd`. |
| `SG_MAPPER_WORKERS` | Réplicas de mapper para Q4. |
| `SG_LINKER_WORKERS` | Réplicas de linker para Q4. |
| `SG_DETECTOR_WORKERS` | Réplicas de detector para Q4. |
| `PREFETCH_COUNT` | Prefetch de RabbitMQ en workers que lo soportan. |

Variables de ejecución:

| Variable | Uso |
|---|---|
| `KEEP_CONTAINERS=1` | Mantiene containers luego de un test específico. |
| `TEST_CLIENT_WAIT_TIMEOUT` | Timeout para esperar clientes en tests. Ej: `3600s`. |
| `TEST_SMOKE_DEADLINE_SECONDS` | Deadline de smoke checks en `make test`. |
| `LOG_COLOR=never` | Desactiva color en logs formateados. |
| `LOG_ARGS` | Argumentos extra para `docker compose logs`. |
| `FLOW_LOG_EVERY_MESSAGES` | Cada cuantos mensajes DATA de RabbitMQ loguear progreso por publisher/consumer. Default: `100`. |
| `FLOW_LOG_EVERY_BYTES` | Cada cuantos bytes de RabbitMQ loguear progreso por publisher/consumer. Default: `8388608`. |
| `WORKER_LOG_EVERY_MESSAGES` | Cada cuantos batches DATA loguear progreso semantico dentro de cada worker. Default: `100`. |
| `FLOW_LOG_ENABLED=0` | Desactiva los logs de flujo generados por el middleware. |
| `CHUNK_LOG_EVERY` | Cada cuantos chunks cliente/gateway loguear progreso. Default: `100`. |
| `RESULT_LOG_EVERY` | Cada cuantas lineas de resultado gateway->cliente loguear progreso. Default: `100`. |

## Monitor y recuperación

El bloque `monitor` de `config/main-config.yaml` habilita las réplicas que
detectan caídas por heartbeat y recuperan containers con `docker start`:

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
```

Los procesos envían su heartbeat UDP a todas las réplicas. Los monitores usan
elección Bully para coordinar cuál de ellos ejecuta las recuperaciones. Para
acceder al daemon del host, los containers de monitor montan
`/var/run/docker.sock`.

Para deshabilitarlos en un escenario:

```yaml
monitor:
  enabled: false
```

## Outputs

Los resultados se escriben en `data/output/`:

```text
data/output/results_q1_<client_id>.csv
data/output/results_q2_<client_id>.csv
data/output/results_q3_<client_id>.csv
data/output/results_q4_<client_id>.csv
data/output/results_q5_<client_id>.csv
```

Cada test específico ejecuta su validador correspondiente:

```bash
Q1_DATASET_DIR=data/datasets/LI-Mini Q1_DATASET_TRANS=LI-Mini_Trans.csv python3 scripts/validate_q1_output.py
Q2_DATASET_DIR=data/datasets/LI-Mini Q2_DATASET_TRANS=LI-Mini_Trans.csv python3 scripts/validate_q2_output.py
Q3_DATASET_DIR=data/datasets/LI-Mini Q3_DATASET_TRANS=LI-Mini_Trans.csv python3 scripts/validate_q3_output.py
Q5_DATASET_DIR=data/datasets/LI-Mini Q5_DATASET_TRANS=LI-Mini_Trans.csv python3 scripts/validate_q5_output.py
```

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

Comando típico para una corrida de performance con containers vivos:

```bash
KEEP_CONTAINERS=1 CLIENTS=3 TEST_DATASET=LI-Small USD_WORKERS=4 \
Q5_FORMAT_WORKERS=4 Q5_USD_WORKERS=4 \
SG_MAPPER_WORKERS=2 SG_LINKER_WORKERS=2 SG_DETECTOR_WORKERS=2 \
Q3_BARRIER_WORKERS=3 PREFETCH_COUNT=50 \
TEST_CLIENT_WAIT_TIMEOUT=3600s TEST_SMOKE_DEADLINE_SECONDS=3600 \
make test
```

## Escenarios

El proyecto permite generar compose desde escenarios:

```bash
make scenario 1
make scenario config/scenarios/4.yaml
```

También existe un selector interactivo:

```bash
make switch
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
