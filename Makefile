SHELL := /bin/bash
PWD := $(shell pwd)
MAIN_PROJECT ?= $(shell basename "$(PWD)" | tr '[:upper:]' '[:lower:]')
COMPOSE_FILE := docker-compose.yaml
TEST_COMPOSE_FILE := docker-compose.test.yaml
TEST_PROJECT := distribuidos-test
MAIN_CONFIG_FILE ?= config/main-config.yaml
TEST_CONFIG_FILE ?= config/test-config.yaml
PYTHON ?= $(if $(wildcard venv/bin/python),venv/bin/python,python3)
LOG_PYTHON ?= $(PYTHON) -u
COMPOSE_SCRIPT := scripts/generate_compose.py
LOG_FORMATTER := scripts/pretty_logs.py
LOG_COLOR ?= always
LOG_ARGS ?=
SCENARIO_ARG := $(word 2,$(MAKECMDGOALS))
TEST_Q1_SUCCESS_PATTERN := Forward pass successful - Mate | filter=Q1
TEST_CLIENT_DONE_PATTERN := client_results_finished
TEST_Q2_EOF_PATTERN := gateway_eof | prefix=Q2|
TEST_Q4_EOF_PATTERN := gateway_eof | prefix=Q4|
TEST_CLIENT_WAIT_TIMEOUT ?= 4600s
TEST_SMOKE_DEADLINE_SECONDS ?= 600
SCENARIOS_DIR := config/scenarios
RABBIT_SCREEN_URL ?= http://localhost:15672

config:
	$(PYTHON) $(COMPOSE_SCRIPT) --config $(MAIN_CONFIG_FILE) --output $(COMPOSE_FILE) --skip-test-output
.PHONY: config

test-config:
	$(PYTHON) $(COMPOSE_SCRIPT) --config $(TEST_CONFIG_FILE) --test-output $(TEST_COMPOSE_FILE) --skip-output
.PHONY: test-config

scenario:
	@scenario="$(if $(SCENARIO),$(SCENARIO),$(SCENARIO_ARG))"; \
	if [ -z "$$scenario" ]; then \
		echo "Usage: make scenario <scenario-name-or-path>"; \
		echo "Examples: make scenario 1"; \
		echo "          make scenario config/scenarios/4.yaml"; \
		exit 2; \
	fi; \
	if [ -f "$$scenario" ]; then \
		config_file="$$scenario"; \
	elif [ -f "$(SCENARIOS_DIR)/$$scenario" ]; then \
		config_file="$(SCENARIOS_DIR)/$$scenario"; \
	elif [ -f "$(SCENARIOS_DIR)/$$scenario.yaml" ]; then \
		config_file="$(SCENARIOS_DIR)/$$scenario.yaml"; \
	else \
		echo "Scenario not found: $$scenario" >&2; \
		exit 2; \
	fi; \
	$(PYTHON) $(COMPOSE_SCRIPT) --config "$$config_file" --test-output $(TEST_COMPOSE_FILE) --skip-output
.PHONY: scenario

ifneq ($(filter scenario,$(MAKECMDGOALS)),)
ifneq ($(SCENARIO_ARG),)
$(SCENARIO_ARG):
	@:
endif
endif

up:
	$(MAKE) config
	mkdir -p data/output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f $(COMPOSE_FILE) up --build --remove-orphans --detach
	docker compose -f $(COMPOSE_FILE) logs --follow --timestamps --no-color | $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR)
.PHONY: up

build:
	$(MAKE) config
	docker compose -f $(COMPOSE_FILE) build
.PHONY: build

rebuild:
	$(MAKE) config
	$(MAKE) down
	mkdir -p data/output
	docker compose -f $(COMPOSE_FILE) build --no-cache
	docker compose -f $(COMPOSE_FILE) up --detach --remove-orphans
.PHONY: rebuild

down:
	-docker compose -f $(COMPOSE_FILE) stop -t 5
	-docker compose -f $(COMPOSE_FILE) down --remove-orphans
	@if [ -f "$(TEST_COMPOSE_FILE)" ]; then \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) stop -t 5 || true; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) down --volumes --remove-orphans || true; \
	fi
	@for project in "$(MAIN_PROJECT)" "$(TEST_PROJECT)"; do \
		containers=$$(docker ps -aq --filter "label=com.docker.compose.project=$$project"); \
		if [ -n "$$containers" ]; then docker rm -f $$containers >/dev/null; fi; \
		networks=$$(docker network ls -q --filter "label=com.docker.compose.project=$$project"); \
		if [ -n "$$networks" ]; then docker network rm $$networks >/dev/null 2>&1 || true; fi; \
	done
.PHONY: down

hard-down:
	-docker compose -f $(COMPOSE_FILE) kill
	-docker compose -f $(COMPOSE_FILE) down --volumes --remove-orphans --timeout 20
	@if [ -f "$(TEST_COMPOSE_FILE)" ]; then \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) kill || true; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) down --volumes --remove-orphans --timeout 20 || true; \
	fi
.PHONY: hard-down

clean-state:
	mkdir -p data/output
	rm -f data/output/results_q*.csv
	find data/datasets -mindepth 1 -maxdepth 1 -type d -name 'client-*' -exec rm -rf {} +
.PHONY: clean-state

logs-test:
	docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) logs -f --timestamps --no-color $(LOG_ARGS) | $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR)
.PHONY: logs-test

logs:
	docker compose -f $(COMPOSE_FILE) logs --timestamps --no-color $(LOG_ARGS) | $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR)
.PHONY: logs

rabbit-screen:
	@bash -lc 'set -euo pipefail; \
		url="$(RABBIT_SCREEN_URL)"; \
		echo "Opening RabbitMQ queues: $$url"; \
		echo "Credentials: guest / guest"; \
		if command -v xdg-open >/dev/null 2>&1; then \
			xdg-open "$$url" >/dev/null 2>&1 & \
		elif command -v open >/dev/null 2>&1; then \
			open "$$url" >/dev/null 2>&1 & \
		elif command -v python3 >/dev/null 2>&1; then \
			python3 -m webbrowser "$$url"; \
		else \
			echo "No browser opener found. Open manually: $$url" >&2; \
			exit 1; \
		fi'
.PHONY: rabbit-screen

stats:
	docker stats
.PHONY: stats

# Dataset used by the full `make test` run and `make expected`.
DATASET ?= HI-Small

# Precompute the per-dataset reference results (data/datasets/<DATASET>/expected_results/).
# Use FORCE=1 to regenerate. The expensive Q4 graph is computed once here, not per run.
expected:
	$(PYTHON) scripts/precompute_expected.py --dataset $(DATASET) $(if $(FORCE),--force)
.PHONY: expected

# Full end-to-end test: kills leftover TP containers, runs the WHOLE pipeline
# (Q1-Q5) from the full test config, validates every query's output for every
# client against the precomputed reference, and prints a metrics footer
# (per-query PASS/FAIL + time, container count, peak CPU/RAM) as the last lines.
# Parametrize like the test-qN targets, e.g.:
#   DATASET=HI-Medium CLIENTS=2 USD_WORKERS=4 PREFETCH_COUNT=50 make test
test:
	@echo ">>> regenerating $(TEST_COMPOSE_FILE) from $(TEST_CONFIG_FILE)"
	$(PYTHON) $(COMPOSE_SCRIPT) --config $(TEST_CONFIG_FILE) \
		$(if $(DATASET),--dataset $(DATASET)) \
		$(if $(USD_WORKERS),--filter-usd-workers $(USD_WORKERS)) \
		$(if $(Q2_SUM_WORKERS),--sum-q2-workers $(Q2_SUM_WORKERS)) \
		$(if $(Q5_FORMAT_WORKERS),--filter-q5-format-workers $(Q5_FORMAT_WORKERS)) \
		$(if $(Q5_USD_WORKERS),--filter-q5-usd-workers $(Q5_USD_WORKERS)) \
		$(if $(SG_MAPPER_WORKERS),--sg-mapper-workers $(SG_MAPPER_WORKERS)) \
		$(if $(SG_LINKER_WORKERS),--sg-linker-workers $(SG_LINKER_WORKERS)) \
		$(if $(SG_DETECTOR_WORKERS),--sg-detector-workers $(SG_DETECTOR_WORKERS)) \
		$(if $(Q4_FILTER_WORKERS),--q4-filter-workers $(Q4_FILTER_WORKERS)) \
		$(if $(Q4_SUM_WORKERS),--q4-sum-workers $(Q4_SUM_WORKERS)) \
		$(if $(Q4_JOINER_WORKERS),--q4-joiner-workers $(Q4_JOINER_WORKERS)) \
		$(if $(Q4_AGGREGATOR_WORKERS),--q4-aggregator-workers $(Q4_AGGREGATOR_WORKERS)) \
		$(if $(Q4_DEDUPER_WORKERS),--q4-deduper-workers $(Q4_DEDUPER_WORKERS)) \
		$(if $(Q3_BARRIER_WORKERS),--q3-barrier-workers $(Q3_BARRIER_WORKERS)) \
		$(if $(PREFETCH_COUNT),--prefetch $(PREFETCH_COUNT)) \
		$(if $(CLIENTS),--clients $(CLIENTS)) \
		--test-output $(TEST_COMPOSE_FILE) --skip-output
	DATASET=$(DATASET) DATASET_ROOT=data/datasets LOG_COLOR=$(LOG_COLOR) \
	TEST_PROJECT=$(TEST_PROJECT) MAIN_PROJECT=$(MAIN_PROJECT) \
	TEST_COMPOSE_FILE=$(TEST_COMPOSE_FILE) \
	TEST_CLIENT_WAIT_TIMEOUT=$(TEST_CLIENT_WAIT_TIMEOUT) \
	KEEP_CONTAINERS=$(KEEP_CONTAINERS) \
	$(LOG_PYTHON) scripts/run_full_test.py
.PHONY: test

Q1_DATASET ?= HI-Small
test-q1:
	@echo ">>> regenerating $(TEST_COMPOSE_FILE) for Q1 (dataset=$(Q1_DATASET))"
	@$(PYTHON) $(COMPOSE_SCRIPT) --preset q1-test --dataset $(Q1_DATASET) \
		$(if $(USD_WORKERS),--filter-usd-workers $(USD_WORKERS)) \
		$(if $(PREFETCH_COUNT),--prefetch $(PREFETCH_COUNT)) \
		$(if $(CLIENTS),--clients $(CLIENTS)) \
		--test-output $(TEST_COMPOSE_FILE) --skip-output
	@bash -lc 'set -euo pipefail; \
		compose="docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE)"; \
		cleanup() { $$compose down --volumes --remove-orphans >/dev/null 2>&1; }; \
		if [ -z "$(KEEP_CONTAINERS)" ]; then \
			trap cleanup EXIT; \
		else \
			echo "KEEP_CONTAINERS set — containers will remain after test"; \
			echo "  logs:  $$compose logs -f <service>"; \
			echo "  down:  $$compose down --volumes --remove-orphans"; \
		fi; \
		cleanup; \
		mkdir -p data/output; \
		rm -f data/output/results_q*.csv; \
		start_time=$$SECONDS; \
		echo "Starting Q1 flow test (dataset=$(Q1_DATASET), CLIENTS=$(or $(CLIENTS),1))..."; \
		$$compose up --build --remove-orphans --detach; \
		$$compose logs --follow --timestamps --no-color \
			| $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR) & LOG_PID=$$!; \
		clients="$$($$compose config --services | grep "^client_" | tr "\n" " ")"; \
		if [ -z "$$clients" ]; then echo "no client services found" >&2; kill $$LOG_PID 2>/dev/null || true; exit 2; fi; \
		timeout $(TEST_CLIENT_WAIT_TIMEOUT) $$compose wait $$clients >/dev/null || true; \
		kill $$LOG_PID 2>/dev/null || true; \
		wait $$LOG_PID 2>/dev/null || true; \
		elapsed=$$((SECONDS - start_time)); \
		echo ""; \
		echo "Client finished in $${elapsed}s"; \
		Q1_DATASET_DIR=data/datasets/$(Q1_DATASET) \
		Q1_DATASET_TRANS=$(Q1_DATASET)_Trans.csv \
			$(PYTHON) scripts/validate_q1_output.py \
			&& echo "✓ Q1 test PASSED ($${elapsed}s)" \
			|| { echo "✗ Q1 test FAILED ($${elapsed}s)"; exit 1; }'
.PHONY: test-q1

Q2_DATASET ?= HI-Small
Q2_SUM_WORKERS ?= 4
CLIENTS ?= 2
test-q2:
	@echo ">>> regenerating $(TEST_COMPOSE_FILE) for Q2 (dataset=$(Q2_DATASET))"
	@$(PYTHON) $(COMPOSE_SCRIPT) --preset q2-test --dataset $(Q2_DATASET) \
		$(if $(USD_WORKERS),--filter-usd-workers $(USD_WORKERS)) \
		$(if $(Q2_SUM_WORKERS),--sum-q2-workers $(Q2_SUM_WORKERS)) \
		$(if $(PREFETCH_COUNT),--prefetch $(PREFETCH_COUNT)) \
		$(if $(CLIENTS),--clients $(CLIENTS)) \
		--test-output $(TEST_COMPOSE_FILE) --skip-output
	@bash -lc 'set -euo pipefail; \
		compose="docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE)"; \
		cleanup() { $$compose down --volumes --remove-orphans >/dev/null 2>&1; }; \
		if [ -z "$(KEEP_CONTAINERS)" ]; then \
			trap cleanup EXIT; \
		else \
			echo "KEEP_CONTAINERS set — containers will remain after test"; \
			echo "  logs:  $$compose logs -f <service>"; \
			echo "  down:  $$compose down --volumes --remove-orphans"; \
		fi; \
		cleanup; \
		mkdir -p data/output; \
		rm -f data/output/results_q2_*.csv; \
		start_time=$$SECONDS; \
		echo "Starting Q2 flow test (dataset=$(Q2_DATASET), Q2_SUM_WORKERS=$(or $(Q2_SUM_WORKERS),1), CLIENTS=$(or $(CLIENTS),1))..."; \
		$$compose up --build --remove-orphans --detach; \
		$$compose logs --follow --timestamps --no-color \
			| $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR) & LOG_PID=$$!; \
		clients="$$($$compose config --services | grep "^client_" | tr "\n" " ")"; \
		if [ -z "$$clients" ]; then echo "no client services found" >&2; kill $$LOG_PID 2>/dev/null || true; exit 2; fi; \
		timeout $(TEST_CLIENT_WAIT_TIMEOUT) $$compose wait $$clients >/dev/null || true; \
		kill $$LOG_PID 2>/dev/null || true; \
		wait $$LOG_PID 2>/dev/null || true; \
		elapsed=$$((SECONDS - start_time)); \
		echo ""; \
		echo "Client finished in $${elapsed}s"; \
		Q2_DATASET_DIR=data/datasets/$(Q2_DATASET) \
		Q2_DATASET_TRANS=$(Q2_DATASET)_Trans.csv \
			$(PYTHON) scripts/validate_q2_output.py \
			&& echo "✓ Q2 test PASSED ($${elapsed}s)" \
			|| { echo "✗ Q2 test FAILED ($${elapsed}s)"; exit 1; }'
.PHONY: test-q2

Q3_DATASET ?= HI-Small
test-q3:
	@echo ">>> regenerating $(TEST_COMPOSE_FILE) for Q3 (dataset=$(Q3_DATASET))"
	@$(PYTHON) $(COMPOSE_SCRIPT) --preset q3-test --dataset $(Q3_DATASET) \
		$(if $(USD_WORKERS),--filter-usd-workers $(USD_WORKERS)) \
		$(if $(Q3_BARRIER_WORKERS),--q3-barrier-workers $(Q3_BARRIER_WORKERS)) \
		$(if $(PREFETCH_COUNT),--prefetch $(PREFETCH_COUNT)) \
		$(if $(CLIENTS),--clients $(CLIENTS)) \
		--test-output $(TEST_COMPOSE_FILE) --skip-output
	@bash -lc 'set -euo pipefail; \
		compose="docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE)"; \
		cleanup() { $$compose down --volumes --remove-orphans >/dev/null 2>&1; }; \
		if [ -z "$(KEEP_CONTAINERS)" ]; then \
			trap cleanup EXIT; \
		else \
			echo "KEEP_CONTAINERS set — containers will remain after test"; \
			echo "  logs:  $$compose logs -f <service>"; \
			echo "  down:  $$compose down --volumes --remove-orphans"; \
		fi; \
		cleanup; \
		mkdir -p data/output; \
		rm -f data/output/results_q*.csv; \
		start_time=$$SECONDS; \
		echo "Starting Q3 flow test (dataset=$(Q3_DATASET), CLIENTS=$(or $(CLIENTS),1))..."; \
		$$compose up --build --remove-orphans --detach; \
		$$compose logs --follow --timestamps --no-color \
			| $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR) & LOG_PID=$$!; \
		clients="$$($$compose config --services | grep "^client_" | tr "\n" " ")"; \
		if [ -z "$$clients" ]; then echo "no client services found" >&2; kill $$LOG_PID 2>/dev/null || true; exit 2; fi; \
		timeout $(TEST_CLIENT_WAIT_TIMEOUT) $$compose wait $$clients >/dev/null || true; \
		kill $$LOG_PID 2>/dev/null || true; \
		wait $$LOG_PID 2>/dev/null || true; \
		elapsed=$$((SECONDS - start_time)); \
		echo ""; \
		echo "Client finished in $${elapsed}s"; \
		Q3_DATASET_DIR=data/datasets/$(Q3_DATASET) \
		Q3_DATASET_TRANS=$(Q3_DATASET)_Trans.csv \
			$(PYTHON) scripts/validate_q3_output.py \
			&& echo "✓ Q3 test PASSED ($${elapsed}s)" \
			|| { echo "✗ Q3 test FAILED ($${elapsed}s)"; exit 1; }'
.PHONY: test-q3

Q5_DATASET ?= HI-Small
Q5_FORMAT_WORKERS ?= 3
Q5_USD_WORKERS ?= 3
USD_WORKERS ?=
PREFETCH_COUNT ?=
test-q5:
	@echo ">>> regenerating $(TEST_COMPOSE_FILE) for Q5 (dataset=$(Q5_DATASET))"
	@$(PYTHON) $(COMPOSE_SCRIPT) --preset q5-test --dataset $(Q5_DATASET) \
		$(if $(Q5_FORMAT_WORKERS),--filter-q5-format-workers $(Q5_FORMAT_WORKERS)) \
		$(if $(Q5_USD_WORKERS),--filter-q5-usd-workers $(Q5_USD_WORKERS)) \
		$(if $(USD_WORKERS),--filter-usd-workers $(USD_WORKERS)) \
		$(if $(PREFETCH_COUNT),--prefetch $(PREFETCH_COUNT)) \
		$(if $(CLIENTS),--clients $(CLIENTS)) \
		--test-output $(TEST_COMPOSE_FILE) --skip-output
	@bash -lc 'set -euo pipefail; \
		compose="docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE)"; \
		cleanup() { $$compose down --volumes --remove-orphans >/dev/null 2>&1; }; \
		if [ -z "$(KEEP_CONTAINERS)" ]; then \
			trap cleanup EXIT; \
		else \
			echo "KEEP_CONTAINERS set — containers will remain after test"; \
			echo "  logs:  $$compose logs -f <service>"; \
			echo "  down:  $$compose down --volumes --remove-orphans"; \
		fi; \
		cleanup; \
		mkdir -p data/output; \
		rm -f data/output/results_q*.csv; \
		start_time=$$SECONDS; \
		echo "Starting Q5 flow test (dataset=$(Q5_DATASET), Q5_USD_WORKERS=$(or $(Q5_USD_WORKERS),1), PREFETCH_COUNT=$(or $(PREFETCH_COUNT),1), CLIENTS=$(or $(CLIENTS),1))..."; \
		$$compose up --build --remove-orphans --detach; \
		$$compose logs --follow --timestamps --no-color \
			| $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR) & LOG_PID=$$!; \
		clients="$$($$compose config --services | grep "^client_" | tr "\n" " ")"; \
		if [ -z "$$clients" ]; then echo "no client services found" >&2; kill $$LOG_PID 2>/dev/null || true; exit 2; fi; \
		timeout $(TEST_CLIENT_WAIT_TIMEOUT) $$compose wait $$clients >/dev/null || true; \
		kill $$LOG_PID 2>/dev/null || true; \
		wait $$LOG_PID 2>/dev/null || true; \
		elapsed=$$((SECONDS - start_time)); \
		echo ""; \
		echo "Client finished in $${elapsed}s"; \
		Q5_DATASET_DIR=data/datasets/$(Q5_DATASET) \
		Q5_DATASET_TRANS=$(Q5_DATASET)_Trans.csv \
			$(PYTHON) scripts/validate_q5_output.py \
			&& echo "✓ Q5 test PASSED ($${elapsed}s)" \
			|| { echo "✗ Q5 test FAILED ($${elapsed}s)"; exit 1; }'
.PHONY: test-q5

test-unit:
	docker build -f Dockerfile.test -t test-runner .
	docker run --rm test-runner
.PHONY: test-unit

# Corre la suite de tests dentro del contenedor de Dockerfile.test (mismo
# entorno que CI). Por defecto corre tests/; se puede acotar con PYTEST_ARGS.
# Uso: make run-tests
#      make run-tests PYTEST_ARGS="tests/gateway -q"
PYTEST_ARGS ?= tests/
run-tests:
	docker build -f Dockerfile.test -t test-runner .
	docker run --rm test-runner python -m pytest $(PYTEST_ARGS) --tb=short
.PHONY: run-tests
