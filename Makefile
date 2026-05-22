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

test-q5:
	bash -lc 'set -euo pipefail; \
		compose="docker compose -f docker-compose.test-q5.yaml"; \
		cleanup() { $$compose down --volumes --remove-orphans >/dev/null 2>&1; }; \
		trap cleanup EXIT; \
		cleanup; \
		mkdir -p data/output; \
		start_time=$$SECONDS; \
		echo "Starting Q5 flow test (LI-Small dataset)..."; \
		$$compose up --build --remove-orphans --detach; \
		echo "Waiting for client to finish (timeout 1800s)..."; \
		deadline=$$((SECONDS + 1800)); \
		while ! $$compose logs client_0 2>&1 | grep -q "client_results_finished"; do \
			if [ "$$SECONDS" -ge "$$deadline" ]; then echo "TIMEOUT waiting for client"; exit 124; fi; \
			sleep 5; \
		done; \
		elapsed=$$((SECONDS - start_time)); \
		echo "Client finished in $${elapsed}s"; \
		sleep 3; \
		echo "Validating Q5 output..."; \
		python3 scripts/validate_q5_output.py && echo "✓ Q5 test PASSED ($${elapsed}s)" || echo "✗ Q5 test FAILED ($${elapsed}s)"; \
		echo ""; \
		echo "=== client_0 logs ==="; \
		$$compose logs client_0; \
		echo "=== gateway logs ==="; \
		$$compose logs gateway; \
		echo "=== filter_q5_format_0 logs ==="; \
		$$compose logs filter_q5_format_0; \
		echo "=== filter_q5_usd_0 logs ==="; \
		$$compose logs filter_q5_usd_0; \
		echo "=== aggregation_q5_0 logs ==="; \
		$$compose logs aggregation_q5_0; \
		echo "=== join_q5 logs ==="; \
		$$compose logs join_q5'
.PHONY: test-q5

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
