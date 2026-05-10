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
	docker compose -f $(COMPOSE_FILE) config --quiet
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
