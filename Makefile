.PHONY: help build up down clean pull-models migrate reset trigger test logs

COMPOSE := docker compose

OLLAMA_MODEL     ?= qwen2.5:3b
POSTGRES_USER    ?= rico
POSTGRES_DB      ?= rico
MINIO_ACCESS_KEY ?= minioadmin
MINIO_SECRET_KEY ?= minioadmin
MINIO_BUCKET     ?= rico-raw
LIMIT            ?= 5

# The commit the pipeline code runs from — recorded in pipeline_runs.git_sha
# and passed into the Airflow containers (the .git dir is not in the image).
GIT_SHA := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
export GIT_SHA

help:
	@echo "RICO pipeline targets:"
	@echo "  build        build the custom Airflow image (Airflow 2.10 + ML stack)"
	@echo "  up           start the full stack (Postgres, MinIO, Ollama, Airflow)"
	@echo "  pull-models  pull the Ollama model into the container (run once)"
	@echo "  trigger      trigger the DAG          (make trigger LIMIT=5)"
	@echo "  migrate      apply migrations/002 to an already-running rico DB"
	@echo "  reset        truncate all pipeline tables + clear the MinIO bucket"
	@echo "  test         run the unit tests"
	@echo "  down         stop services (volumes preserved)"
	@echo "  clean        stop services and wipe volumes (full reset)"
	@echo "  logs         tail compose logs"

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d --wait postgres minio ollama
	$(COMPOSE) up -d minio-init ollama-init
	$(COMPOSE) up -d airflow-init airflow-scheduler airflow-webserver
	@echo ""
	@echo "Airflow UI  -> http://localhost:8080  (login: admin / admin)"
	@echo "MinIO UI    -> http://localhost:9001"

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

pull-models:
	$(COMPOSE) exec ollama ollama pull $(OLLAMA_MODEL)

# Apply the traceability migration to a DB whose volume already exists
# (002 auto-runs only on a fresh volume). Every statement in 002 is idempotent,
# so this is safe to re-run.
migrate:
	$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) \
	  < migrations/002_traceability.sql

# Truncate every pipeline table and clear the MinIO bucket. CASCADE handles
# the run_id foreign keys back to pipeline_runs.
reset:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "TRUNCATE TABLE pipeline_runs, audit_results, pipeline_metrics, \
	   screens_metadata, screens_embeddings, screens_review_queue, screens_eval \
	   RESTART IDENTITY CASCADE;"
	$(COMPOSE) exec minio mc alias set local http://minio:9000 $(MINIO_ACCESS_KEY) $(MINIO_SECRET_KEY) >/dev/null 2>&1 || true
	$(COMPOSE) exec minio mc rm --recursive --force local/$(MINIO_BUCKET)/ >/dev/null 2>&1 || true
	@echo "pipeline state truncated"

trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger rico_pipeline \
	  --conf '{"limit": $(LIMIT)}'

test:
	.venv/bin/python -m pytest -q

logs:
	$(COMPOSE) logs -f --tail=100
