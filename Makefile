.PHONY: help up down build shell logs test run ingest dbt-run dbt-test publish docs \
        airflow-up airflow-down bi-up bi-down generate generate-large scale-test clean
.DEFAULT_GOAL := help

COMPOSE := docker compose
PIPELINE := $(COMPOSE) exec pipeline
REFERENCE_DATE ?= 2025-03-16

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Build and start the pipeline container in the background
	$(COMPOSE) up -d --build

down: ## Stop and remove containers
	$(COMPOSE) down

build: ## Rebuild the image without starting the container
	$(COMPOSE) build

shell: ## Open an interactive shell inside the container
	$(PIPELINE) bash

logs: ## Tail container logs
	$(COMPOSE) logs -f

run: ## Full pipeline (ingest + dbt build) for REFERENCE_DATE (default 2025-03-16)
	$(PIPELINE) python -m pipeline run --reference-date $(REFERENCE_DATE)

ingest: ## Bronze ingest only for REFERENCE_DATE
	$(PIPELINE) python -m pipeline ingest --reference-date $(REFERENCE_DATE)

dbt-run: ## dbt run only for REFERENCE_DATE
	$(PIPELINE) python -m pipeline dbt-run --reference-date $(REFERENCE_DATE)

dbt-test: ## dbt test (data-quality gates) for REFERENCE_DATE
	$(PIPELINE) python -m pipeline dbt-test --reference-date $(REFERENCE_DATE)

publish: ## Export Gold marts to consumable Parquet products in gold/
	$(PIPELINE) python -m pipeline publish --reference-date $(REFERENCE_DATE)

docs: ## Generate + serve dbt docs (lineage) on :8081
	$(PIPELINE) dbt docs generate --project-dir dbt --profiles-dir dbt

test: ## Run the pytest suite inside the container
	$(PIPELINE) pytest tests/ -v

airflow-up: ## Start the Airflow orchestration layer (http://localhost:8080)
	$(COMPOSE) --profile airflow up -d

airflow-down: ## Stop the Airflow orchestration layer
	$(COMPOSE) --profile airflow down

bi-up: ## Start Metabase BI on the published products (http://localhost:3000)
	$(COMPOSE) --profile bi up -d

bi-down: ## Stop the Metabase BI layer
	$(COMPOSE) --profile bi down

generate: ## Generate a small synthetic dataset (10k rows) into /tmp/generated
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 10000 --days 30 --merchants 100 --seed 42 --out /tmp/generated

generate-large: ## Generate a large synthetic dataset (1M rows) into /tmp/generated-large
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 1000000 --days 90 --merchants 500 --seed 42 --out /tmp/generated-large

scale-test: generate-large ## Generate 1M rows then run the pipeline against them
	$(PIPELINE) sh -c 'DATA_PATH=/tmp/generated-large python -m pipeline run --reference-date $(REFERENCE_DATE)'

clean: ## Remove generated artifacts and stop the containers
	-$(PIPELINE) rm -rf /tmp/generated /tmp/generated-large
	$(COMPOSE) down -v
