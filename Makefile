SHELL := /bin/bash
PWD := $(shell pwd)
COMPOSE_FILE := docker-compose.yaml
TEST_COMPOSE_FILE := docker-compose.test.yaml
TEST_PROJECT := mla-forward-pass-test
CONFIG_FILE ?= config/main-config.yaml
CONDA_ENV ?= distribuidos
PYTHON ?= conda run -n $(CONDA_ENV) python
LOG_PYTHON ?= conda run --no-capture-output -n $(CONDA_ENV) python -u
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
	$(PYTHON) $(COMPOSE_SCRIPT) --config $(CONFIG_FILE)
.PHONY: config

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
	$(PYTHON) $(COMPOSE_SCRIPT) --config "$$config_file"
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
	docker compose -f $(COMPOSE_FILE) stop -t 5
	docker compose -f $(COMPOSE_FILE) down
	@if [ -f "$(TEST_COMPOSE_FILE)" ]; then \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) stop -t 5 || true; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) down --volumes --remove-orphans || true; \
	fi
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
	$(MAKE) config
	bash -lc 'set -euo pipefail; \
		log_file=/tmp/$(TEST_PROJECT).log; \
		logs_pid=""; \
		tail_pid=""; \
		cleanup() { docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) down --volumes --remove-orphans >/dev/null; }; \
		stop_logs() { \
			if [ -n "$$tail_pid" ]; then kill "$$tail_pid" >/dev/null 2>&1 || true; wait "$$tail_pid" >/dev/null 2>&1 || true; fi; \
			if [ -n "$$logs_pid" ]; then kill "$$logs_pid" >/dev/null 2>&1 || true; wait "$$logs_pid" >/dev/null 2>&1 || true; fi; \
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
		mkdir -p data/output; \
		rm -f data/output/results_q*.csv; \
		cleanup; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) config --quiet; \
		clients="$$(docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) config --services | grep "^client_" | tr "\n" " ")"; \
		if [ -z "$$clients" ]; then echo "no client services generated" >&2; exit 2; fi; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) up --build --remove-orphans --detach; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) logs --follow --timestamps --no-color > "$$log_file" 2>&1 & \
		logs_pid=$$!; \
		tail -n +1 -f "$$log_file" | $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR) >&2 & \
		tail_pid=$$!; \
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

test-q2:
	bash -lc 'set -euo pipefail; \
		cleanup() { docker compose -f docker-compose.test.yaml down --volumes --remove-orphans >/dev/null 2>&1; }; \
		trap cleanup EXIT; \
		cleanup; \
		mkdir -p data/output; \
		echo "Starting Q2 flow test..."; \
		docker compose -f docker-compose.test.yaml up --build --remove-orphans --detach; \
		echo "Waiting for services to be ready..."; \
		sleep 10; \
		echo "Checking client logs for completion..."; \
		timeout 120s sh -c '\''docker compose -f docker-compose.test.yaml logs --follow 2>&1 | grep -m1 "client_shutdown\|client_results_finished"'\''; \
		sleep 5; \
		echo "Validating Q2 output..."; \
		python3 scripts/validate_q2_output.py && echo "✓ Q2 test PASSED" || echo "✗ Q2 test FAILED"; \
		echo ""; \
		echo "=== client_0 logs ==="; \
		docker compose -f docker-compose.test.yaml logs client_0; \
		echo "=== gateway logs ==="; \
		docker compose -f docker-compose.test.yaml logs gateway; \
		echo "=== join_q2 logs ==="; \
		docker compose -f docker-compose.test.yaml logs join_q2; \
		echo "=== aggregation_q2_0 logs ==="; \
		docker compose -f docker-compose.test.yaml logs aggregation_q2_0'
.PHONY: test-q2

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
