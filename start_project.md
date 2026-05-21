# Start here — running the RICO pipeline

How to build, start, and verify the project in its **current state (Part A)**.
For the task split and team contracts see `TASK_SPLIT.md`; for the assignment
spec see `README.md`.

> **What works right now:** the infrastructure, the schema, and the
> `init_run → ingest → parse` stages. The `embed_image`, `embed_text`,
> `extract`, `load`, `audit`, and `eval` stages are **stubs** — Parts B and C
> have not been built yet. A full pipeline run is therefore *expected to fail
> at the embed stage* (see [What you should see](#5-what-you-should-see)).

---

## 1. Prerequisites

- **Docker Desktop** running.
- **~18–20 GB free disk**, and the Docker VM given ≥ 25 GB
  (*Docker Desktop → Settings → Resources → Disk*). See `TASK_SPLIT.md`.
- Free local ports: `8080` (Airflow), `5432` (Postgres), `9000`/`9001` (MinIO),
  `11434` (Ollama).
- For the unit tests only: Python 3.11+. A `.venv` already exists in this repo.

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
make build      # build the custom Airflow image (Airflow 2.10 + ML stack)
make up         # start Postgres, MinIO, Ollama, and Airflow
```

- `make build` is the slow step — a multi-GB image, **~10–20 min** the first
  time. After that it is cached.
- `make up` brings everything up and prints the UI URLs. Give Airflow
  **~1 minute** to finish starting.
- The Ollama model (`qwen2.5:3b`, ~1.9 GB) downloads in the background. It is
  **not needed for Part A** — only the future `extract` stage uses it.

---

## 4. Open Airflow and trigger the pipeline

Open the Airflow UI: **http://localhost:8080** — login **admin / admin**.

You should see a DAG named **`rico_pipeline`**. Trigger it with 5 screens:

```bash
make trigger LIMIT=5
```

(Or use the ▶ button in the UI. Use `make trigger LIMIT=50` for a larger run.)

Watch progress in the UI: **rico_pipeline → Grid** or **Graph**.

---

## 5. What you should see

| Stage | Expected result |
|---|---|
| `init_run` | ✅ success — creates the `pipeline_runs` row |
| `ingest` | ✅ success — streams 5 screens into MinIO + `screens_metadata` |
| `parse` | ✅ success — writes `screens/{id}.txt` to MinIO |
| `embed_image` / `embed_text` / `extract` | ❌ **fail — `NotImplementedError`** (Part B not built) |
| `load` / `audit` / `eval` | ⬜ skipped (upstream failed) |

**The overall run ends "failed" — that is correct for Part A.** The three embed
stages are deliberate stubs. Part A is proven by the rows and blobs the first
three stages produce (next section).

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
missing (e.g. a fresh clone on a teammate's machine):

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

## 10. Troubleshooting

**The `rico_pipeline` DAG is missing, or shows "import error".**
Check the scheduler: `docker compose logs airflow-scheduler | tail -50`. An
import error usually means a dependency is missing from the image — rebuild
with `make build`.

**Schema changes to `002_traceability.sql` are not showing up.**
`002` runs automatically only on a *fresh* Postgres volume. Either
`make clean && make up` (this **wipes data**) or `make migrate` on the running
DB.

**Image build fails with `exec ... : input/output error`.**
A transient Docker glitch. Retry; if it persists, `docker rmi` the offending
image and let the build re-pull it.

**`make build` fails on a pip dependency conflict.**
The lab's ML pins (`datasets<3`, `huggingface_hub<0.24`) sit on top of
Airflow's dependency set — a resolver clash is possible and would need a pin
adjustment in `pyproject.toml`.

**`ingest` is slow on the first run.**
It streams the RICO dataset from HuggingFace; the first run downloads dataset
shards. They are cached in the `hf-cache` volume for subsequent runs.

**A port is already in use.**
Free `8080`, `5432`, `9000`, `9001`, or `11434`, or stop whatever is using it.
