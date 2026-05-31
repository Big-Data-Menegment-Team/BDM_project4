# Running the RICO Pipeline

This project re-implements the RICO lab notebook as an Airflow DAG with:

- idempotent loading,
- run-level traceability,
- duplicate-detection audit circuit breaker,
- recall@5 evaluation,
- persisted health/data-quality metrics,
- Slack notifications (best effort).

Pipeline shape:

`init_run -> ingest -> parse -> [embed_image, embed_text, extract] -> load -> audit -> eval`

## 1. Prerequisites

- Docker Desktop with `docker compose`
- Python 3.11+ (for local unit tests)
- Optional: `make` (commands below include direct `docker compose` alternatives)

## 2. One-time Configuration

Copy environment template:

```bash
cp .env.example .env
```

Set Slack webhook for notifications:

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

`.env` is gitignored and must not be committed.

## 3. Build and Start the Stack

If `make` is available:

```bash
make build
make up
```

Direct `docker compose` flow:

```bash
docker compose build
docker compose up -d --wait postgres minio
docker compose up -d minio-init
docker compose up -d airflow-init airflow-scheduler airflow-webserver
```

Open Airflow UI: `http://localhost:8080` (admin/admin).

## 4. Trigger the DAG

Default `limit=5`:

```bash
docker compose exec airflow-scheduler airflow dags trigger rico_pipeline
```

Wait for completion:

```bash
docker compose exec postgres psql -U rico -d rico -P pager=off -c \
  "SELECT run_id,status,limit_param,started_at,ended_at FROM pipeline_runs ORDER BY started_at DESC LIMIT 3;"
```

## 5. Expected Task Outcomes

Healthy run:

- `init_run`, `ingest`, `parse`, `embed_image`, `embed_text`, `extract`, `load`, `audit`, `eval` succeed.
- `pipeline_runs.status = succeeded`.
- `audit_results.passed = true`.
- `screens_eval` has one row for the run with recall@5.

Audit-halt run (duplicate corruption case):

- `audit` fails, downstream `eval` is skipped.
- `pipeline_runs.status = paused-by-audit`.
- `audit_results.passed = false` with duplicate keys in `details`.

## 6. Verify Core Requirements

### 6.1 Traceability columns are non-null

```bash
docker compose exec postgres psql -U rico -d rico -P pager=off -c \
  "SELECT 'metadata' AS t, count(*) AS bad_rows FROM screens_metadata WHERE run_id IS NULL OR source_fingerprint IS NULL
   UNION ALL
   SELECT 'embeddings', count(*) FROM screens_embeddings WHERE run_id IS NULL OR source_fingerprint IS NULL
   UNION ALL
   SELECT 'review_queue', count(*) FROM screens_review_queue WHERE run_id IS NULL OR source_fingerprint IS NULL;"
```

All counts should be `0`.

### 6.2 Metrics persisted per run

```bash
docker compose exec postgres psql -U rico -d rico -P pager=off -c \
  "SELECT metric_name, metric_value FROM pipeline_metrics ORDER BY created_at DESC LIMIT 50;"
```

### 6.3 Eval persisted

```bash
docker compose exec postgres psql -U rico -d rico -P pager=off -c \
  "SELECT run_id, embedding_model_version, n_queries, recall_at_5, created_at
   FROM screens_eval ORDER BY created_at DESC LIMIT 5;"
```

## 7. Idempotency Check

Re-triggering with same `LIMIT` must not add destination rows.

```bash
docker compose exec postgres psql -U rico -d rico -tAc "SELECT count(*) FROM screens_metadata;"
docker compose exec postgres psql -U rico -d rico -tAc "SELECT count(*) FROM screens_embeddings;"
docker compose exec airflow-scheduler airflow dags trigger rico_pipeline
# wait for completion, then rerun the two count queries
```

Counts should remain unchanged.

## 8. Audit Interpretation

The audit checks duplicates on:

- `screens_embeddings`: `(screen_id, model_name, model_version, embedding_kind)`
- `screens_metadata` (run-scoped): duplicate `screen_id` within the run

If duplicates are found:

- audit writes a failed `audit_results` row with duplicate keys in `details`,
- `pipeline_runs.status` becomes `paused-by-audit`,
- DAG does not proceed to eval.

Check latest audit:

```bash
docker compose exec postgres psql -U rico -d rico -P pager=off -c \
  "SELECT run_id, audit_name, passed, details FROM audit_results ORDER BY created_at DESC LIMIT 1;"
```

## 9. Metric Interpretation

Common metric groups in `pipeline_metrics`:

- `health.final_status`: encoded run status (`succeeded`, `failed`, `paused-by-audit`).
- `health.total_run_duration_sec`: total runtime.
- `health.task_duration_sec.<task_id>`: per-task durations.
- `health.task_retries.<task_id>`: retry counts.
- `health.task_rows_in.<task_id>`, `health.task_rows_out.<task_id>`: task data flow.
- `dq.metadata.*`: extraction coverage, confidence ratio, review queue ratio, cardinality checks.
- `dq.embeddings.*`: total embedding rows, dimensionality checks, zero-norm ratio.
- `dq.eval.recall_at_5`: retrieval quality from eval stage.

The scheduler log also prints an end-of-run summary line:

`run=<run_id> stage=summary status=... duration_s=... metadata_rows=... recall_at_5=...`

## 10. Slack Notifications

For each run, Slack posts:

- run started,
- audit failed (only when audit fails),
- run finished with final status and summary.

Slack posting is best effort: notification failures are logged but do not fail pipeline execution.

## 11. Unit Tests

If `make` is available:

```bash
make test
```

Without `make`:

```bash
py -3 -m pytest -q
```

## 12. Useful Commands

| Command | Purpose |
|---|---|
| `make up` | Start stack (if `make` is available) |
| `make down` | Stop stack |
| `make clean` | Stop and wipe volumes |
| `make trigger LIMIT=5` | Trigger DAG with limit |
| `make reset` | Truncate pipeline tables and clear MinIO bucket |
| `make migrate` | Apply migration 002 on existing DB |
| `docker compose ps` | Show service state |
| `docker compose logs -f airflow-scheduler` | Tail scheduler logs |

