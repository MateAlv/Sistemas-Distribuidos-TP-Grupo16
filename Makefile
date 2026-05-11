SHELL := /bin/bash
PWD := $(shell pwd)
COMPOSE_FILE := docker-compose.yaml
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
		trap "docker compose -f $(COMPOSE_FILE) stop rabbitmq >/dev/null" EXIT; \
		docker compose -f $(COMPOSE_FILE) config --quiet; \
		docker compose -f $(COMPOSE_FILE) up -d rabbitmq; \
		for i in $$(seq 1 30); do \
			status=$$(docker inspect -f "{{.State.Health.Status}}" rabbitmq 2>/dev/null || true); \
			if [[ "$$status" == "healthy" ]]; then \
				echo "rabbitmq_healthy"; \
				break; \
			fi; \
			if [[ "$$i" == "30" ]]; then \
				echo "rabbitmq did not become healthy"; \
				exit 1; \
			fi; \
			echo "rabbitmq_status=$$status"; \
			sleep 2; \
		done; \
		PYTHONPATH=src conda run -n base python scripts/forward_pass_test.py'
.PHONY: test

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
