SHELL := /bin/bash
PWD := $(shell pwd)
COMPOSE_FILE := docker-compose.yaml
TEST_COMPOSE_FILE := docker-compose.test.yaml
TEST_PROJECT := mla-forward-pass-test
TEST_Q1_SUCCESS_PATTERN := Forward pass successful - Mate | filter=Q1
TEST_SUM_SUCCESS_PATTERN := sum_forward_eof_to_aggregator
SCENARIOS_DIR := config/scenarios

up:
	mkdir -p data/output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f $(COMPOSE_FILE) up --build --remove-orphans --detach
	docker compose -f $(COMPOSE_FILE) logs --follow
.PHONY: up

down:
	docker compose -f $(COMPOSE_FILE) stop -t 5
	docker compose -f $(COMPOSE_FILE) down
.PHONY: down

logs:
	docker compose -f $(COMPOSE_FILE) logs
.PHONY: logs

test:
	bash -lc 'set -euo pipefail; \
		cleanup() { docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) down --volumes --remove-orphans >/dev/null; }; \
		trap cleanup EXIT; \
		cleanup; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) config --quiet; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) up --build --remove-orphans --detach; \
		timeout 120s bash -lc '\''set -o pipefail; \
			docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) logs --follow 2>&1 | \
			awk '\''\'\''/$(TEST_Q1_SUCCESS_PATTERN)/ { q1=1; print } /$(TEST_SUM_SUCCESS_PATTERN)/ { sum=1; print } q1 && sum { exit 0 }'\''\'\'''\''; \
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
		python scripts/validate_q1_output.py && echo "✓ Q1 test PASSED" || echo "✗ Q1 test FAILED"; \
		docker compose -f docker-compose.test.yaml logs'
.PHONY: test-q1

switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, una sola réplica de cada elemento"
	@echo "2) Múltiples clientes, una sola réplica de cada elemento"
	@echo "3) Múltiples clientes, sum replicado, un solo aggregator"
	@echo "4) Múltiples clientes, múltiples réplicas"
	@echo "5) Múltiples clientes, múltiples réplicas, nombres al azar"
	@read -p "Selecciona uno [1-5]: " option;	\
	cp $(SCENARIOS_DIR)/$${option}.yaml $(COMPOSE_FILE)
.PHONY: switch

test-unit:
	docker build -f Dockerfile.test -t test-runner .
	docker run --rm test-runner
.PHONY: test-unit
