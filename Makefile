SHELL := /bin/bash
PWD := $(shell pwd)
COMPOSE_FILE := docker-compose.yaml
TEST_COMPOSE_FILE := docker-compose.test.yaml
TEST_PROJECT := mla-forward-pass-test
TEST_Q1_SUCCESS_PATTERN := Forward pass successful - Mate | filter=Q1
TEST_Q2_SUM_PARTIAL_PATTERN := sum_forward_partial | configuration=Q2
TEST_Q3_SUM_PARTIAL_PATTERN := sum_forward_partial | configuration=Q3
TEST_Q2_SUM_EOF_PATTERN := sum_forward_eof_to_aggregator | configuration=Q2
TEST_Q3_SUM_EOF_PATTERN := sum_forward_eof_to_aggregator | configuration=Q3
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
		cleanup; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) config --quiet; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) up --build --remove-orphans --detach; \
		docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) logs --follow > "$$log_file" 2>&1 & \
		logs_pid=$$!; \
		tail -n +1 -f "$$log_file" >&2 & \
		tail_pid=$$!; \
		deadline=$$((SECONDS + 120)); \
		wait_for_pattern "$(TEST_Q1_SUCCESS_PATTERN)"; \
		wait_for_pattern "$(TEST_Q2_SUM_PARTIAL_PATTERN)"; \
		wait_for_pattern "$(TEST_Q3_SUM_PARTIAL_PATTERN)"; \
		wait_for_pattern "$(TEST_Q2_SUM_EOF_PATTERN)"; \
		wait_for_pattern "$(TEST_Q3_SUM_EOF_PATTERN)"; \
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
