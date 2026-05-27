# Running the RICO pipeline

> **What works right now:** Parts A and B are implemented end-to-end —
> `init_run → ingest → parse → [embed_image, embed_text, extract] → load`
> all succeed. Re-triggering with the same `LIMIT` is **idempotent**
> (verified across multiple consecutive runs).
>
> The `audit` and `eval` stages are still **Part C stubs** (`NotImplementedError`),
> so a full DAG run currently ends with `audit` red and `eval` skipped — that's
> expected until Part C lands.

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

First-time run: ~2–10 min, dominated by Ollama generating the LLM extraction
and the initial CLIP/SBERT model downloads. Subsequent runs cache both.

---

## 5. What you should see

| Stage | Expected result |
|---|---|
| `init_run` | ✅ success — creates the `pipeline_runs` row |
| `ingest` | ✅ success — streams 5 screens into MinIO + `screens_metadata` |
| `parse` | ✅ success — writes `screens/{id}.txt` to MinIO |
| `embed_image` | ✅ success — open-clip ViT-B-32 vectors staged as `screens/{id}.clip.npz` |
| `embed_text` | ✅ success — SBERT MiniLM-L6-v2 vectors staged as `screens/{id}.sbert.npz` |
| `extract` | ✅ success — Ollama (`qwen2.5:3b`) JSON staged as `screens/{id}.extract.json` |
| `load` | ✅ success — single idempotent transaction writes embeddings + extraction in place |
| `audit` | ❌ **fail — `NotImplementedError`** (Part C not built yet) |
| `eval` | ⬜ skipped (upstream failed) |

---

## 6. Verify the results

**Postgres** — every row carries traceability columns:

```bash
# One row per trigger, with git_sha and model versions.
docker compose exec postgres psql -U rico -d rico -c \
  "SELECT run_id, status, limit_param, git_sha, prompt_version, started_at FROM pipeline_runs ORDER BY started_at DESC;"

# Ingested screens with LLM extraction filled in by load.
docker compose exec postgres psql -U rico -d rico -c \
  "SELECT screen_id, prompt_version, confidence, left(source_fingerprint, 12) AS fp FROM screens_metadata ORDER BY screen_id;"

# Embeddings: 2 rows per screen (one image, one text) - 10 rows for LIMIT=5.
docker compose exec postgres psql -U rico -d rico -c \
  "SELECT model_name, model_version, embedding_kind, count(*) FROM screens_embeddings GROUP BY 1,2,3;"

# DoD §3.2: every destination row has non-null run_id + source_fingerprint.
docker compose exec postgres psql -U rico -d rico -c \
  "SELECT 'metadata'    AS t, count(*) FROM screens_metadata     WHERE run_id IS NULL OR source_fingerprint IS NULL
   UNION ALL SELECT 'embeddings' , count(*) FROM screens_embeddings   WHERE run_id IS NULL OR source_fingerprint IS NULL
   UNION ALL SELECT 'review_queue', count(*) FROM screens_review_queue WHERE run_id IS NULL OR source_fingerprint IS NULL;"
```

**MinIO** — open **http://localhost:9001** (login **minioadmin / minioadmin**),
bucket **`rico-raw`**, prefix **`screens/`**. For each screen you should see
six objects:

| Object | Written by | Purpose |
|---|---|---|
| `{id}.png` | `ingest` | raw screenshot |
| `{id}.json` | `ingest` | raw view-hierarchy |
| `{id}.txt` | `parse` | extracted text representation |
| `{id}.clip.npz` | `embed_image` | open-clip vector + fingerprint |
| `{id}.sbert.npz` | `embed_text` | SBERT vector + fingerprint |
| `{id}.extract.json` | `extract` | LLM extraction (`{ok, payload, fingerprint}`) |

---

## 7. Check idempotency

Re-running the same `LIMIT` must **not** create new rows in any destination
table. The interesting tables to watch are `screens_metadata` (PK-bound) and
`screens_embeddings` (no unique constraint - idempotency is policed by the
load stage's screen-scoped delete-then-insert):

```bash
docker compose exec postgres psql -U rico -d rico -tAc "SELECT count(*) FROM screens_metadata;"
docker compose exec postgres psql -U rico -d rico -tAc "SELECT count(*) FROM screens_embeddings;"
make trigger LIMIT=5
# wait for the run to complete (audit will fail — that's Part C), then re-count.
# Both numbers must be unchanged.
docker compose exec postgres psql -U rico -d rico -tAc "SELECT count(*) FROM screens_metadata;"
docker compose exec postgres psql -U rico -d rico -tAc "SELECT count(*) FROM screens_embeddings;"
```

Why screen_id-scoped (not run_id-scoped) DELETE: each Airflow trigger
generates a fresh `dag_run_id` → fresh `run_id`. A `DELETE WHERE run_id = …`
would never touch the previous run's rows, so the table would grow by 10
on every re-trigger. `DELETE WHERE screen_id = ANY(…)` correctly wipes the
prior rows for the screens this run is processing.

---

## 8. Unit tests (no Docker needed)

```bash
make test
```

Runs **18 tests**: the original `fingerprint` and `parse` tests, plus
`tests/test_load_sql.py` — static checks that the load stage's DELETE
statements are screen-scoped (the regression guard for §7's idempotency
rule). If the `.venv` is missing (e.g. a fresh clone):

```bash
python3 -m venv .venv && .venv/bin/pip install pytest
```

---

## 8a. Failure-mode handling (Part B)

The extract and load stages catch every realistic upstream failure
per screen and route the affected screen to `screens_review_queue`
instead of crashing the task. Possible review-queue reasons:

| `reason` value | Cause | Caught in |
|---|---|---|
| `text_missing: …` | `parse` never wrote `screens/{id}.txt` (or MinIO lost it) | `rico/tasks/extract.py` |
| `llm_bad_json: …` / `llm_failed: …` | Ollama returned non-JSON, timed out, or 5xx | `rico/tasks/extract.py` |
| `extract_missing` | extract didn't produce an artifact for this screen | `rico/tasks/load.py` |
| LLM error string verbatim | extract staged `{ok: false}` with the error | `rico/tasks/load.py` |

extract's end-of-stage log line gives a one-glance breakdown:

```
stage=extract complete screens=5 ok=4 failed=1 text_missing=1 llm_failed=0
```

`grep reason=text_missing` (or `llm_failed`, etc.) on the task log isolates
one failure mode at a time. Every review-queue row still has a non-null
`run_id` and `source_fingerprint` (deterministic per failure mode), so the
DoD §3.2 traceability invariant holds even on the unhappy path.

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
