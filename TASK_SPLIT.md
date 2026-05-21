# Task Split — RICO Production Pipeline (Project 4)

## The project in one line

Turn the lab notebook (`notebook.ipynb`) into a scheduled, idempotent, observable,
auditable **Airflow DAG** with 7 stages:

```
ingest → parse → [embed_image, embed_text, extract] → load → audit → eval
```

The three middle tasks run in parallel. Plus four "graded meat" features:
traceability (§3.2), the duplicate-detection audit (§3.3), observability metrics (§3.4),
and Slack notifications (§3.5).

---

## The 3-way split

### 🟢 Part A — Foundation + Traceability + DAG spine  *(start immediately, blocks nobody)*

- Infrastructure: add Airflow to `docker-compose.yml` + `Makefile`
- Project skeleton: `pyproject.toml`, the `rico/` package
- DB schema migration `002_traceability.sql` — all new tables + `run_id` / `source_fingerprint` columns
- Shared helper modules: `config`, `db`, `storage`, `fingerprint`
- The thin DAG file with **all 7 task stubs wired**
- The `ingest` and `parse` tasks
- **Owns deliverable §3.2 — record traceability**

### 🔵 Part B — Compute stages + idempotency  *(starts once Part A's schema + helpers + DAG skeleton land)*

- `embed_image` (CLIP), `embed_text` (SBERT), `extract` (LLM + versioned prompt file)
- `load` — idempotent `INSERT ... ON CONFLICT` into all destination tables, writing
  `run_id` + `source_fingerprint`, routing bad LLM JSON to the review queue
- **Owns most of §3.1 + the idempotency Definition-of-Done items**

### 🟣 Part C — Audit + Eval + Observability + Slack + README  *(starts once Part A's schema lands; integrates after B)*

- `audit` task — duplicate-detection circuit breaker → `audit_results`
- `eval` task — recall@5
- Observability — `pipeline_metrics` (health + data quality), end-of-run log summary
- Slack notifications — 3 moments (run started, audit failed, run finished)
- The project `README`
- **Owns deliverables §3.3, §3.4, §3.5**

### Definition-of-Done coverage

| DoD item (README §5) | Owner |
|---|---|
| `make up` → DAG appears in Airflow UI, no errors | A |
| `LIMIT=5` populates all tables + `pipeline_runs` + `pipeline_metrics` row | A + B + C |
| Re-trigger `LIMIT=5` → no new rows (idempotent) | A (ingest) + B (load) |
| Corrupt `screens_embeddings` → audit fails, eval skipped | C |
| Every row has non-null `run_id` + `source_fingerprint` | A (schema) + B (load) |
| End-of-run log line: health + data-quality summary | C |
| Slack messages for success / failure / audit-halt | C |
| README explains run + metrics + audit interpretation | C |

---

## Part A — detailed guideline

### Files to create

```
BDM_project4/
├── pyproject.toml              # project + deps (replaces requirements.txt role)
├── .env.example                # Slack webhook placeholder, overridable creds
├── .gitignore                  # .venv, __pycache__, .env, logs/
├── docker-compose.yml          # EXTEND: add Airflow services
├── Makefile                    # EXTEND: `up` brings up Airflow too
├── migrations/
│   ├── 001_init.sql            # exists — leave it
│   └── 002_traceability.sql    # NEW ← the crux of Part A
├── dags/
│   └── rico_pipeline.py        # NEW — thin DAG, orchestration only
└── rico/                       # NEW — the package; business logic lives here
    ├── __init__.py
    ├── config.py               # connection strings, model versions, defaults
    ├── db.py                   # psycopg helper, pipeline_runs create/finalize, git_sha
    ├── storage.py              # boto3/MinIO client + put/get
    ├── fingerprint.py          # sha256 helpers
    └── tasks/
        ├── __init__.py
        ├── ingest.py           # Part A
        ├── parse.py            # Part A
        ├── embed_image.py      # stub for B
        ├── embed_text.py       # stub for B
        ├── extract.py          # stub for B
        ├── load.py             # stub for B
        ├── audit.py            # stub for C
        └── eval.py             # stub for C
```

### Order of work

#### Phase 1 — publish contracts (do first, commit early — this unblocks B and C)

1. **`migrations/002_traceability.sql`** — the spine. The README tip says it outright:
   *"Start with traceability schema, not code."*

   - **New `pipeline_runs`**: `run_id UUID PK`, `dag_run_id TEXT`, `started_at`, `ended_at`,
     `status`, `limit_param INT`, `git_sha TEXT`, `clip_version`, `sbert_version`,
     `llm_model`, `prompt_version`.
   - **New `audit_results`**: `id BIGSERIAL`, `run_id UUID → pipeline_runs`,
     `audit_name TEXT`, `passed BOOL`, `details JSONB`, `created_at`.
   - **New `pipeline_metrics`**: `id BIGSERIAL`, `run_id UUID → pipeline_runs`,
     `metric_name TEXT`, `metric_value DOUBLE PRECISION` (+ optional `metric_detail JSONB`),
     `created_at`.
   - **ALTER existing tables**: add `run_id UUID REFERENCES pipeline_runs` +
     `source_fingerprint TEXT` to `screens_metadata`, `screens_embeddings`,
     `screens_review_queue`; add `run_id` to `screens_eval`.
   - ⚠️ `002` only runs on a **fresh Postgres volume** (it is mounted into
     `docker-entrypoint-initdb.d`). The first `make up` after adding it needs a
     `make clean` first. Tell the team.

2. **`rico/config.py`** — lift constants from notebook cell 4 (`POSTGRES_DSN`, `MINIO_*`,
   `OLLAMA_*`) and model-version strings from cells 19 / 24 / 28 (`CLIP_MODEL_VERSION`,
   `SBERT_MODEL_VERSION`, `PROMPT_VERSION`). Add `DEFAULT_LIMIT = 5`. Read overrides from
   env vars so Docker can inject them.

3. **`rico/db.py`, `rico/storage.py`, `rico/fingerprint.py`** — publish the **function
   signatures** (stubs are fine for now):
   - `db.py`: `connection()`, `create_pipeline_run(dag_run_id, limit, ...) -> run_id`,
     `finalize_pipeline_run(run_id, status)`, `git_sha()`.
   - `storage.py`: `s3_client()`, `put_object(key, body)`, `get_object(key) -> bytes`.
   - `fingerprint.py`: `sha256_bytes(b) -> str`.

4. **`dags/rico_pipeline.py`** — the thin DAG: define all 7 tasks (calling `rico.tasks.*`
   functions), wire `ingest → parse → [embed_image, embed_text, extract] → load → audit →
   eval` with the 3 middle tasks parallel, expose `LIMIT` as a DAG param. Stub the
   not-yet-owned task functions with `raise NotImplementedError` so the DAG **parses** in
   the Airflow UI immediately.

> Once Phase 1 is committed → ping teammates. **B and C can start now.**

#### Phase 2 — Part A's own tasks

5. **`rico/tasks/ingest.py`** — translate notebook cells 9–13. Stream the RICO dataset,
   take the **first `LIMIT` screens** (drop the fixed `chosen_screens.txt` — `LIMIT` is the
   new control). For each: `PUT` PNG + hierarchy JSON to MinIO, compute
   `source_fingerprint = sha256(png_bytes)`, `INSERT INTO screens_metadata ...
   ON CONFLICT (screen_id) DO UPDATE` writing `run_id` + `source_fingerprint`. Idempotent —
   re-running with the same `LIMIT` creates no new rows.

6. **`rico/tasks/parse.py`** — translate notebook cell 15: `parse_hierarchy()` +
   `text_representation()`. Define the **output contract** B depends on (e.g. a
   `{screen_id: text_rep}` map via XCom, or B recomputes from MinIO — your choice;
   document it).

7. **Infrastructure** — extend `docker-compose.yml` with Airflow (recommend
   **LocalExecutor**, not Celery — no Redis, lighter, simpler for a 3-person project) and a
   custom Airflow image that pip-installs the ML stack. Extend `Makefile` so `make up`
   brings up the whole stack and the DAG shows in the UI.

### Contracts Part A must publish (share with B and C)

- The full `002` schema — column names B and C will read/write.
- How `run_id` propagates: created in `ingest` (or a tiny pre-task), passed to every
  downstream task via XCom.
- `rico.tasks.*` function signatures — what each task receives and returns.
- The `parse → embed/extract` data hand-off format.

### Part A Definition of Done

`make up` → DAG appears in the Airflow UI with no parse errors; triggering with `LIMIT=5`
creates a `pipeline_runs` row and populates `screens_metadata` with non-null `run_id` +
`source_fingerprint`; re-running with `LIMIT=5` adds **zero** new rows.

---

## Notebook → task mapping (reference for all parts)

| Stage | Notebook cells | Owner |
|---|---|---|
| ingest | 9–13 | A |
| parse | 15–17 | A |
| embed_image | 19–22 | B |
| embed_text | 24–26 | B |
| extract | 28–31 | B |
| load (idempotent writes) | the `INSERT` cells, rewritten with `ON CONFLICT` | B |
| audit | new — README §3.3 | C |
| eval | 41–42 (recall@k) | C |
| search helpers (eval reuse) | 33, 38 | C |

---

## Part A — built (status & resolved contracts)

Part A is implemented. Resolved decisions:

| Decision | Choice |
|---|---|
| Airflow version | 2.10.5, LocalExecutor (no Redis, no separate worker) |
| `pipeline_runs` lifecycle | a dedicated `init_run` task creates the row; DAG `on_success`/`on_failure` callbacks finalize it |
| parse → embed/extract handoff | `parse` writes `screens/{id}.txt` to MinIO |

### Schema change B & C must know

`migrations/002_traceability.sql` **drops the composite primary key** on
`screens_embeddings` and replaces it with a surrogate `id`. The tuple
`(screen_id, model_name, model_version, embedding_kind)` is no longer
unique-constrained (verified: 0 unique constraints on the table).

- **Part B `load`:** cannot `INSERT ... ON CONFLICT` on `screens_embeddings` —
  use a **scoped delete-then-insert**. `screens_metadata` keeps its `screen_id`
  PK, so it still uses `ON CONFLICT (screen_id) DO UPDATE`.
- **Part C `audit`:** the duplicate it must detect is now physically possible —
  the audit is the only thing policing that tuple.

### Task contract (the DAG calls these)

Every task takes `run_id`; `ingest` also takes `limit`. To find a run's
screens: `SELECT screen_id, ... FROM screens_metadata WHERE run_id = %s`.

| Function | Status |
|---|---|
| `rico.tasks.ingest.run_ingest(run_id, limit) -> list[int]` | built |
| `rico.tasks.parse.run_parse(run_id) -> dict` | built |
| `rico.tasks.embed_image.run_embed_image(run_id)` | stub — Part B |
| `rico.tasks.embed_text.run_embed_text(run_id)` | stub — Part B |
| `rico.tasks.extract.run_extract(run_id)` | stub — Part B |
| `rico.tasks.load.run_load(run_id)` | stub — Part B |
| `rico.tasks.audit.run_audit(run_id)` | stub — Part C |
| `rico.tasks.eval.run_eval(run_id)` | stub — Part C |

Each stub file's docstring states its full contract.

### Shared helpers (import these — do not re-create)

- `rico.config` — connection settings, model-version strings, `DEFAULT_LIMIT`.
- `rico.db` — `connection(register_pgvector=False)`, `create_pipeline_run`,
  `finalize_pipeline_run`, `git_sha`.
- `rico.storage` — `s3_client`, `put_bytes`, `get_bytes`, `object_exists`,
  and the key builders `png_key` / `hierarchy_key` / `text_key`.
- `rico.fingerprint` — `sha256_bytes`, `sha256_text`.

