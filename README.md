# Running the RICO pipeline

> **What works right now:** the infrastructure, the schema, and the
> `init_run → ingest → parse` stages. The `embed_image`, `embed_text`,
> `extract`, `load`, `audit`, and `eval` stages are **stubs** — Parts B and C
> have not been built yet. A full pipeline run is therefore *expected to fail.

---

## 2. One-time configuration (optional)

The defaults work out of the box — you can skip this. A `.env` is only needed
later for Part C (Slack):

```bash
cp .env.example .env
```

---

## 3. Build and start

```bash
make build      # build the custom Airflow image (Airflow 2.10 + ML stack); SLOW STEP
make up         # start Postgres, MinIO, Ollama (takes time), and Airflow
```

---

## 4. Open Airflow and trigger the pipeline

Open the Airflow UI: **http://localhost:8080** — login **admin / admin**.

You should see a DAG named **`rico_pipeline`**. Trigger it with 5 screens:

```bash
make trigger LIMIT=5
```

---

## 5. What you should see

| Stage | Expected result |
|---|---|
| `init_run` | ✅ success — creates the `pipeline_runs` row |
| `ingest` | ✅ success — streams 5 screens into MinIO + `screens_metadata` |
| `parse` | ✅ success — writes `screens/{id}.txt` to MinIO |
| `embed_image` / `embed_text` / `extract` | ❌ **fail — `NotImplementedError`** (Part B not built) |
| `load` / `audit` / `eval` | ⬜ skipped (upstream failed) |

---

## 6. Verify the results

**Postgres** — every row carries traceability columns:

```bash
# One row per trigger, with git_sha and model versions.
docker compose exec postgres psql -U rico -d rico -c \
  "SELECT run_id, status, limit_param, git_sha, started_at FROM pipeline_runs ORDER BY started_at DESC;"

# Ingested screens — run_id and source_fingerprint must be non-null.
docker compose exec postgres psql -U rico -d rico -c \
  "SELECT screen_id, category, run_id, left(source_fingerprint, 12) AS fingerprint FROM screens_metadata ORDER BY screen_id;"
```

**MinIO** — open **http://localhost:9001** (login **minioadmin / minioadmin**),
bucket **`rico-raw`**, prefix **`screens/`**. For each screen you should see
three objects: `{id}.png`, `{id}.json`, and `{id}.txt`.

---

## 7. Check idempotency

Re-running the same `LIMIT` must **not** create new rows:

```bash
docker compose exec postgres psql -U rico -d rico -tAc "SELECT count(*) FROM screens_metadata;"
make trigger LIMIT=5
# wait for the run to reach 'parse', then count again — it must be unchanged:
docker compose exec postgres psql -U rico -d rico -tAc "SELECT count(*) FROM screens_metadata;"
```

---

## 8. Unit tests (no Docker needed)

```bash
make test
```

Runs the `fingerprint` and `parse` unit tests (12 tests). If the `.venv` is
missing (e.g. a fresh clone):

```bash
python3 -m venv .venv && .venv/bin/pip install pytest
```

---

## 9. Everyday commands

| Command | Does |
|---|---|
| `make up` | Start the whole stack |
| `make down` | Stop services (data **preserved**) |
| `make clean` | Stop services and **wipe all volumes** |
| `make trigger LIMIT=5` | Trigger the DAG |
| `make reset` | Truncate all pipeline tables + clear the MinIO bucket |
| `make migrate` | Apply `002` to an already-running DB |
| `make build` | Rebuild the Airflow image (after Dockerfile/dependency changes) |
| `make logs` | Tail all container logs |
| `make test` | Run the unit tests |
| `make help` | List all targets |

---
