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
MONITOR_TEST_TIMEOUT ?= 30
MONITOR_FAILOVER_TIMEOUT ?= 45
SCENARIO_ARG := $(word 2,$(MAKECMDGOALS))
TEST_Q1_SUCCESS_PATTERN := Forward pass successful - Mate | filter=Q1
TEST_CLIENT_DONE_PATTERN := client_results_finished
TEST_Q2_EOF_PATTERN := gateway_eof | prefix=Q2|
TEST_Q4_EOF_PATTERN := gateway_eof | prefix=Q4|
TEST_CLIENT_WAIT_TIMEOUT ?= 18000s
TEST_DATASET ?= LI-Small
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
	-docker compose -f $(COMPOSE_FILE) down --volumes --remove-orphans
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

CHAOS_SERVICE := chaos_monkey

chaos-kill-random:
	@chaos=$$(docker compose -f $(COMPOSE_FILE) ps --status running --quiet $(CHAOS_SERVICE)); \
	if [ -z "$$chaos" ]; then echo "chaos monkey not running (enable chaos in config and 'make up')" >&2; exit 1; fi; \
	docker exec "$$chaos" python3 -c "from manager import ChaosManager, excluded_from_env, included_from_env; result = ChaosManager(excluded_from_env(), included_from_env()).kill_random_container(); print(result); raise SystemExit(0 if result else 1)"
.PHONY: chaos-kill-random

CHAOS_TARGET := $(if $(CONTAINER),$(CONTAINER),$(word 2,$(MAKECMDGOALS)))
chaos-kill:
	@if [ -z "$(CHAOS_TARGET)" ]; then \
		echo "Usage: make chaos-kill CONTAINER=<service>" >&2; \
		echo "   or: make chaos-kill <service>" >&2; \
		exit 2; \
	fi; \
	if ! docker compose -f $(COMPOSE_FILE) config --services | grep -Fxq "$(CHAOS_TARGET)"; then \
		echo "Unknown compose service: $(CHAOS_TARGET)" >&2; \
		exit 2; \
	fi; \
	chaos=$$(docker compose -f $(COMPOSE_FILE) ps --status running --quiet $(CHAOS_SERVICE)); \
	if [ -z "$$chaos" ]; then echo "chaos monkey not running (enable chaos in config and 'make up')" >&2; exit 1; fi; \
	docker exec "$$chaos" python3 -c "import sys; from manager import ChaosManager; result = ChaosManager().kill_container(sys.argv[1]); print(result); raise SystemExit(0 if result else 1)" "$(CHAOS_TARGET)"
.PHONY: chaos-kill

ifneq ($(filter chaos-kill,$(MAKECMDGOALS)),)
ifneq ($(word 2,$(MAKECMDGOALS)),)
$(word 2,$(MAKECMDGOALS)):
	@:
endif
endif

logs-test:
	docker compose -p $(TEST_PROJECT) -f $(TEST_COMPOSE_FILE) logs -f --timestamps --no-color $(LOG_ARGS) | $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR)
.PHONY: logs-test

logs:
	docker compose -f $(COMPOSE_FILE) logs --timestamps --no-color $(LOG_ARGS) | $(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR)
.PHONY: logs

monitor-logs:
	@services=$$(docker compose -f $(COMPOSE_FILE) config --services | grep '^monitor_'); \
	if [ -z "$$services" ]; then echo "No monitor services are configured" >&2; exit 1; fi; \
	docker compose -f $(COMPOSE_FILE) logs --follow --timestamps --no-color $$services | \
		$(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR)
.PHONY: monitor-logs

monitor-status:
	@services=$$(docker compose -f $(COMPOSE_FILE) config --services | grep '^monitor_'); \
	if [ -z "$$services" ]; then echo "No monitor services are configured" >&2; exit 1; fi; \
	echo "=== Monitor containers ==="; \
	docker compose -f $(COMPOSE_FILE) ps $$services; \
	echo; \
	echo "=== Recent monitor events ==="; \
	docker compose -f $(COMPOSE_FILE) logs --since 5m --timestamps --no-color $$services | \
		$(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR)
.PHONY: monitor-status

monitor-test-recovery:
	@if [ -z "$(CONTAINER)" ]; then \
		echo "Usage: make monitor-test-recovery CONTAINER=<service>" >&2; \
		exit 2; \
	fi
	@bash -lc 'set -euo pipefail; \
		compose=(docker compose -f "$(COMPOSE_FILE)"); \
		target="$(CONTAINER)"; \
		timeout="$(MONITOR_TEST_TIMEOUT)"; \
		all_services=$$("$${compose[@]}" config --services); \
		if ! grep -Fxq "$$target" <<<"$$all_services"; then \
			echo "Unknown compose service: $$target" >&2; exit 2; \
		fi; \
		if [[ "$$target" == monitor_* ]]; then \
			echo "Choose a non-monitor service to test worker recovery" >&2; exit 2; \
		fi; \
		monitors=$$(grep "^monitor_" <<<"$$all_services"); \
		if [ -z "$$monitors" ]; then echo "No monitor services are configured" >&2; exit 1; fi; \
		if [ $$("$${compose[@]}" ps --status running $$monitors --quiet | wc -l | tr -d " ") -eq 0 ]; then \
			echo "No monitor container is running" >&2; exit 1; \
		fi; \
		started_at=$$(date -u +"%Y-%m-%dT%H:%M:%SZ"); \
		echo "Stopping $$target and waiting up to $${timeout}s for monitor recovery..."; \
		"$${compose[@]}" stop -t 0 "$$target" >/dev/null; \
		deadline=$$((SECONDS + timeout)); \
		while (( SECONDS < deadline )); do \
			if [ "$$("$${compose[@]}" ps --status running --quiet "$$target" | wc -l | tr -d " ")" -gt 0 ]; then \
				echo "PASS: $$target was restarted by the monitor"; \
				"$${compose[@]}" logs --since "$$started_at" --timestamps --no-color $$monitors | \
					$(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR); \
				exit 0; \
			fi; \
			sleep 1; \
		done; \
		echo "FAIL: $$target was not restarted within $${timeout}s" >&2; \
		"$${compose[@]}" logs --since "$$started_at" $$monitors >&2; \
		exit 1'
.PHONY: monitor-test-recovery

monitor-test-election:
	@bash -lc 'set -euo pipefail; \
		compose=(docker compose -f "$(COMPOSE_FILE)"); \
		monitors=$$("$${compose[@]}" config --services | grep "^monitor_" | sort -t_ -k2,2n); \
		if [ -z "$$monitors" ]; then echo "No monitor services are configured" >&2; exit 1; fi; \
		leader=$$(printf "%s\n" "$$monitors" | tail -1); \
		successor=$$(printf "%s\n" "$$monitors" | tail -2 | head -1); \
		monitor_count=$$(printf "%s\n" "$$monitors" | wc -l | tr -d " "); \
		if [ "$$leader" = "$$successor" ]; then \
			echo "At least two monitor replicas are required" >&2; exit 1; \
		fi; \
		chaos=$$("$${compose[@]}" ps --status running --quiet "$(CHAOS_SERVICE)"); \
		if [ -z "$$chaos" ]; then \
			echo "Chaos monkey is not running. Enable settings.chaos and start the stack." >&2; \
			exit 1; \
		fi; \
		started_at=$$(date -u +"%Y-%m-%dT%H:%M:%SZ"); \
		echo "Killing leader $$leader through Chaos Monkey..."; \
		docker exec "$$chaos" python3 -c \
			"import sys; from manager import ChaosManager; sys.exit(0 if ChaosManager().kill_container(sys.argv[1]) else 1)" \
			"$$leader"; \
		deadline=$$((SECONDS + $(MONITOR_FAILOVER_TIMEOUT))); \
		while (( SECONDS < deadline )); do \
			logs=$$("$${compose[@]}" logs --since "$$started_at" --no-color $$monitors 2>&1); \
			takeover_epoch=$$(grep -E "monitor_election_won \\| monitor_id=$${successor#monitor_} \\| epoch=[0-9]+" <<<"$$logs" | \
				sed -E "s/.*epoch=([0-9]+).*/\\1/" | tail -1 || true); \
			recovered_epoch=$$(grep -E "monitor_election_won \\| monitor_id=$${leader#monitor_} \\| epoch=[0-9]+" <<<"$$logs" | \
				sed -E "s/.*epoch=([0-9]+).*/\\1/" | tail -1 || true); \
			accepted_count=0; \
			if [ -n "$$recovered_epoch" ]; then \
				accepted_count=$$(grep -E "monitor_coordinator_accepted.*leader_id=$${leader#monitor_}.*epoch=$$recovered_epoch.*announced_epoch=$$recovered_epoch" <<<"$$logs" | \
					sed -E "s/.*monitor_id=([0-9]+).*/\\1/" | sort -u | wc -l | tr -d " " || true); \
			fi; \
			if [ -n "$$takeover_epoch" ] && [ -n "$$recovered_epoch" ] && \
				(( recovered_epoch > takeover_epoch )) && \
				(( accepted_count >= monitor_count - 1 )) && \
				grep -Fq "monitor_recovery_success | node_id=$$leader" <<<"$$logs"; then \
				echo "PASS: $$successor took over at epoch $$takeover_epoch, recovered $$leader, and the cluster reconverged at epoch $$recovered_epoch"; \
				printf "%s\n" "$$logs" | \
					grep -E "monitor_(election|coordinator)|monitor_node_failed.*node_id=$$leader|monitor_recovery_(start|success|failed).*node_id=$$leader" | \
					$(LOG_PYTHON) $(LOG_FORMATTER) --color $(LOG_COLOR); \
				exit 0; \
			fi; \
			sleep 1; \
		done; \
		echo "FAIL: monitor failover and epoch convergence did not complete within $(MONITOR_FAILOVER_TIMEOUT)s" >&2; \
		"$${compose[@]}" logs --since "$$started_at" $$monitors >&2; \
		exit 1'
.PHONY: monitor-test-election

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


expected:
	@if [ -z "$(DATASET)" ]; then \
		echo "Usage: make expected DATASET=<dataset>"; \
		exit 2; \
	fi
	$(PYTHON) scripts/precompute_expected.py --dataset $(DATASET) $(if $(FORCE),--force)
.PHONY: expected

test:
	@echo ">>> regenerating $(TEST_COMPOSE_FILE) from $(TEST_CONFIG_FILE)"
	$(PYTHON) $(COMPOSE_SCRIPT) --config $(TEST_CONFIG_FILE) \
		$(if $(TEST_DATASET),--dataset $(TEST_DATASET)) \
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
	LOG_COLOR=$(LOG_COLOR) \
	TEST_PROJECT=$(TEST_PROJECT) MAIN_PROJECT=$(MAIN_PROJECT) \
	TEST_COMPOSE_FILE=$(TEST_COMPOSE_FILE) \
	TEST_CLIENT_WAIT_TIMEOUT=$(TEST_CLIENT_WAIT_TIMEOUT) \
	KEEP_CONTAINERS=$(KEEP_CONTAINERS) \
	$(LOG_PYTHON) scripts/run_full_test.py
.PHONY: test

Q1_DATASET ?= LI-Small
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

Q2_DATASET ?= LI-Small
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

Q3_DATASET ?= LI-Small
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

Q5_DATASET ?= LI-Small
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

PYTEST_ARGS ?= tests/
run-tests:
	docker build -f Dockerfile.test -t test-runner .
	docker run --rm test-runner python -m pytest $(PYTEST_ARGS) --tb=short
.PHONY: run-tests
