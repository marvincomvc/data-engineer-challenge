.PHONY: help up down build shell logs test query seed generate generate-large run clean
.DEFAULT_GOAL := help

COMPOSE := docker compose
PIPELINE := $(COMPOSE) exec -T pipeline
PIPELINE_DATA := $(COMPOSE) exec -T -e DATA_PATH=$(or $(DATA),/data/input) pipeline
TODAY := $(shell date +%Y-%m-%d)

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf " \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Build and start the container in the background
	$(COMPOSE) up -d --build

down: ## Stop and remove the container
	$(COMPOSE) down

build: ## Rebuild the image without starting the container
	$(COMPOSE) build

shell: ## Open an interactive shell inside the container
	$(PIPELINE) bash

logs: ## Tail container logs
	$(COMPOSE) logs -f

test: ## Run all tests inside the container
	$(PIPELINE) python -m pytest tests/ -v

query: ## Run a SQL file against the warehouse (SQL=queries/example.sql)
	cat $(or $(SQL),queries/example.sql) | \
	$(PIPELINE) python scripts/query.py

run: ## Execute pipeline (REF_DATE=YYYY-MM-DD, DATA=/tmp/generated or /tmp/generated-large)
	$(PIPELINE_DATA) python -m pipeline.cli run --reference-date $(or $(REF_DATE),$(TODAY)) --force

status: ## Check pipeline status for today (or REF_DATE=YYYY-MM-DD)
	$(PIPELINE) python -m pipeline.cli status --reference-date $(or $(REF_DATE),$(TODAY))

generate: ## Generate a small synthetic dataset (10k rows) into /tmp/generated
	$(PIPELINE) python scripts/generate_sample_data.py \
	--rows 10000 --days 30 --merchants 100 --seed 42 --out /tmp/generated

generate-large: ## Generate a large synthetic dataset (1M rows) into /tmp/generated-large
	$(PIPELINE) python scripts/generate_sample_data.py \
	--rows 1000000 --days 90 --merchants 500 --seed 42 --out /tmp/generated-large

clean: ## Remove generated artifacts and stop the container
	-$(PIPELINE) rm -rf /tmp/generated /tmp/generated-large
	$(COMPOSE) down
	rm -f data/warehouse.duckdb data/warehouse.duckdb.wal
