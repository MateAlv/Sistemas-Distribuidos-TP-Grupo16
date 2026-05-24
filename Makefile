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
TEST_CLIENT_WAIT_TIMEOUT ?= 600s
TEST_SMOKE_DEADLINE_SECONDS ?= 600
SCENARIOS_DIR := config/scenarios
RABBIT_SCREEN_URL ?= http://localhost:15672/\#/queues

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

test:
	$(MAKE) test-config
	bash -lc 'set -euo pipefail; \
		log_file="$$(mktemp -t $(TEST_PROJECT).XXXXXX.log)"; \
		log_fifo="$$(mktemp -u -t $(TEST_PROJECT).XXXXXX.fifo)"; \
		logs_pid=""; \
		format_pid=""; \
		cleanup() { \
			docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) down --volumes --remove-orphans >/dev/null; \
			rm -f "$$log_fifo"; \
		}; \
		stop_logs() { \
			if [ -n "$$format_pid" ]; then pkill -TERM -P "$$format_pid" >/dev/null 2>&1 || true; kill "$$format_pid" >/dev/null 2>&1 || true; wait "$$format_pid" >/dev/null 2>&1 || true; fi; \
			if [ -n "$$logs_pid" ]; then pkill -TERM -P "$$logs_pid" >/dev/null 2>&1 || true; kill "$$logs_pid" >/dev/null 2>&1 || true; wait "$$logs_pid" >/dev/null 2>&1 || true; fi; \
		}; \
		wait_for_pattern() { \
			pattern="$$1"; \
			while ! grep -q -- "$$pattern" "$$log_file"; do \
				if [ "$$SECONDS" -ge "$$deadline" ]; then \
					echo "missing smoke pattern: $$pattern" >&2; \
					exit 124; \
				fi; \
				sleep 1; \
			done; \
		}; \
		trap "stop_logs; cleanup" EXIT; \
		: > "$$log_file"; \
		echo "test_log_file=$$log_file" >&2; \
		mkdir -p data/output; \
		rm -f data/output/results_q*.csv; \
		cleanup; \
		mkfifo "$$log_fifo"; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) config --quiet; \
		clients="$$(docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) config --services | grep "^client_" | tr "\n" " ")"; \
		if [ -z "$$clients" ]; then echo "no client services generated" >&2; exit 2; fi; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) up --build --remove-orphans --detach; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) logs --follow --timestamps --no-color > "$$log_fifo" 2>&1 & \
		logs_pid=$$!; \
		$(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR) --tee-file "$$log_file" < "$$log_fifo" >&2 & \
		format_pid=$$!; \
		deadline=$$((SECONDS + $(TEST_SMOKE_DEADLINE_SECONDS))); \
		timeout $(TEST_CLIENT_WAIT_TIMEOUT) docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) wait $$clients >/dev/null; \
		wait_for_pattern "$(TEST_Q1_SUCCESS_PATTERN)"; \
		wait_for_pattern "$(TEST_CLIENT_DONE_PATTERN)"; \
		wait_for_pattern "$(TEST_Q2_EOF_PATTERN)"; \
		wait_for_pattern "$(TEST_Q4_EOF_PATTERN)"; \
		echo "forward_pass_test_success"'
.PHONY: test

test-q1:
	bash -lc 'set -euo pipefail; \
		cleanup() { docker compose -f docker-compose.test.yaml down --volumes --remove-orphans >/dev/null 2>&1; }; \
		trap cleanup EXIT; \
		cleanup; \
		mkdir -p data/output; \
		echo "Starting Q1 flow test..."; \
		docker compose -f docker-compose.test.yaml up --build --remove-orphans --detach; \
		echo "Waiting for services to be ready..."; \
		sleep 10; \
		echo "Checking client logs for completion..."; \
		timeout 120s sh -c '\''docker compose -f docker-compose.test.yaml logs --follow 2>&1 | grep -m1 "client_shutdown\|client_results_finished"'\''; \
		sleep 5; \
		echo "Validating Q1 output..."; \
		python3 scripts/validate_q1_output.py && echo "✓ Q1 test PASSED" || echo "✗ Q1 test FAILED"; \
		echo ""; \
		echo "=== client_0 logs ==="; \
		docker compose -f docker-compose.test.yaml logs client_0; \
		echo "=== gateway logs ==="; \
		docker compose -f docker-compose.test.yaml logs gateway; \
		echo "=== filter_q1_0 logs ==="; \
		docker compose -f docker-compose.test.yaml logs filter_q1_0'
.PHONY: test-q1

Q2_DATASET ?= LI-Mini
Q2_SUM_WORKERS ?=
test-q2:
	@$(PYTHON) $(COMPOSE_SCRIPT) --preset q2-test --dataset $(Q2_DATASET) \
		$(if $(USD_WORKERS),--filter-usd-workers $(USD_WORKERS)) \
		$(if $(Q2_SUM_WORKERS),--sum-q2-workers $(Q2_SUM_WORKERS)) \
		$(if $(PREFETCH_COUNT),--prefetch $(PREFETCH_COUNT)) \
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
		echo "Starting Q2 flow test (preset=q2-test, dataset=$(Q2_DATASET), USD_WORKERS=$(or $(USD_WORKERS),1), Q2_SUM_WORKERS=$(or $(Q2_SUM_WORKERS),1), PREFETCH_COUNT=$(or $(PREFETCH_COUNT),1))..."; \
		$$compose up --build --remove-orphans --detach; \
		clients="$$($$compose config --services | grep "^client_" | tr "\n" " ")"; \
		if [ -z "$$clients" ]; then echo "no client services found" >&2; exit 2; fi; \
		timeout $(TEST_CLIENT_WAIT_TIMEOUT) $$compose wait $$clients >/dev/null; \
		elapsed=$$((SECONDS - start_time)); \
		echo "Client finished in $${elapsed}s"; \
		Q2_DATASET_DIR=data/datasets/client-1/$(Q2_DATASET) \
		Q2_DATASET_TRANS=$(Q2_DATASET)_Trans.csv \
			$(PYTHON) scripts/validate_q2_output.py \
			&& echo "✓ Q2 test PASSED ($${elapsed}s)" \
			|| { echo "✗ Q2 test FAILED ($${elapsed}s)"; exit 1; }; \
		echo ""; \
		echo "=== client_0 logs ==="; $$compose logs client_0; \
		echo "=== gateway logs ==="; $$compose logs gateway; \
		echo "=== sum_q2 logs ==="; $$compose logs $$($$compose config --services | grep "^sum_q2_"); \
		echo "=== aggregation_q2_0 logs ==="; $$compose logs aggregation_q2_0; \
		echo "=== join_q2 logs ==="; $$compose logs join_q2; \
		echo "=== q2_bank_name_joiner logs ==="; $$compose logs q2_bank_name_joiner'
.PHONY: test-q2

Q5_DATASET ?= LI-Mini
Q5_FORMAT_WORKERS ?=
Q5_USD_WORKERS ?=
USD_WORKERS ?=
PREFETCH_COUNT ?=
test-q5:
	@$(PYTHON) $(COMPOSE_SCRIPT) --preset q5-test --dataset $(Q5_DATASET) \
		$(if $(Q5_FORMAT_WORKERS),--filter-q5-format-workers $(Q5_FORMAT_WORKERS)) \
		$(if $(Q5_USD_WORKERS),--filter-q5-usd-workers $(Q5_USD_WORKERS)) \
		$(if $(USD_WORKERS),--filter-usd-workers $(USD_WORKERS)) \
		$(if $(PREFETCH_COUNT),--prefetch $(PREFETCH_COUNT)) \
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
		echo "Starting Q5 flow test (preset=q5-test, dataset=$(Q5_DATASET))..."; \
		$$compose up --build --remove-orphans --detach; \
		clients="$$($$compose config --services | grep "^client_" | tr "\n" " ")"; \
		if [ -z "$$clients" ]; then echo "no client services found" >&2; exit 2; fi; \
		timeout $(TEST_CLIENT_WAIT_TIMEOUT) $$compose wait $$clients >/dev/null; \
		elapsed=$$((SECONDS - start_time)); \
		echo "Client finished in $${elapsed}s"; \
		Q5_DATASET_DIR=data/datasets/client-1/$(Q5_DATASET) \
		Q5_DATASET_TRANS=$(Q5_DATASET)_Trans.csv \
			$(PYTHON) scripts/validate_q5_output.py \
			&& echo "✓ Q5 test PASSED ($${elapsed}s)" \
			|| { echo "✗ Q5 test FAILED ($${elapsed}s)"; exit 1; }; \
		echo ""; \
		echo "=== client_0 logs ==="; $$compose logs client_0; \
		echo "=== gateway logs ==="; $$compose logs gateway; \
		echo "=== filter_q5_format_0 logs ==="; $$compose logs filter_q5_format_0; \
		echo "=== filter_q5_usd_0 logs ==="; $$compose logs filter_q5_usd_0; \
		echo "=== aggregation_q5_0 logs ==="; $$compose logs aggregation_q5_0; \
		echo "=== join_q5 logs ==="; $$compose logs join_q5'
.PHONY: test-q5


switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, una sola réplica de cada elemento"
	@echo "2) Múltiples clientes, una sola réplica de cada elemento"
	@echo "3) Múltiples clientes, sum replicado, un solo aggregator"
	@echo "4) Múltiples clientes, múltiples réplicas"
	@echo "5) Múltiples clientes, múltiples réplicas, datasets mixtos"
	@read -p "Selecciona uno [1-5]: " option;	\
	$(PYTHON) $(COMPOSE_SCRIPT) --config $(SCENARIOS_DIR)/$${option}.yaml
.PHONY: switch

test-unit:
	docker build -f Dockerfile.test -t test-runner .
	docker run --rm test-runner
.PHONY: test-unit
