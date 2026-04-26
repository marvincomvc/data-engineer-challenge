.PHONY: help up down build shell logs test generate generate-large pipeline pipeline-date clean
.DEFAULT_GOAL := help

COMPOSE   := docker compose
# run --rm starts a fresh one-off container per command, so make targets work
# whether or not a long-running container is already up.
PIPELINE  := $(COMPOSE) run --rm pipeline
DATE      ?= $(shell date +%F)

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Build image and run the pipeline (streams logs, exits when done)
	$(COMPOSE) up --build

down: ## Stop and remove containers
	$(COMPOSE) down

build: ## Rebuild the image without starting the container
	$(COMPOSE) build

shell: ## Open an interactive shell inside the container
	$(PIPELINE) bash

logs: ## Tail container logs
	$(COMPOSE) logs -f

test: ## Run all tests inside the container
	$(PIPELINE) python -m pytest tests/ -v

pipeline: ## Run the pipeline for the sample-data reference date (2025-03-04)
	$(PIPELINE) python -m pipeline.main --reference-date 2025-03-04

pipeline-date: ## Run the pipeline for a specific date: make pipeline-date DATE=2025-03-16
	$(PIPELINE) python -m pipeline.main --reference-date $(DATE)

generate: ## Generate a small synthetic dataset (10k rows) into /tmp/generated
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 10000 --days 30 --merchants 100 --seed 42 --out /tmp/generated

generate-large: ## Generate a large synthetic dataset (1M rows) into /tmp/generated-large
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 1000000 --days 90 --merchants 500 --seed 42 --out /tmp/generated-large

clean: ## Remove generated artifacts, DuckDB warehouse, and stop containers
	-$(PIPELINE) rm -rf /data
	-rm -rf data/
	$(COMPOSE) down
