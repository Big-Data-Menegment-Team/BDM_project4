-- 002_traceability.sql — production traceability layer (Project 4, §3.2–§3.4).
--
-- Runs automatically after 001_init.sql on a FRESH Postgres volume (both are
-- mounted into docker-entrypoint-initdb.d). For an already-initialised volume,
-- apply manually with `make migrate`. Every statement is idempotent.

\c rico

-- ---------------------------------------------------------------------------
-- pipeline_runs — one row per DAG run; the anchor of all record traceability.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY,
    dag_run_id      TEXT NOT NULL UNIQUE,            -- Airflow's run id
    triggered_by    TEXT,                            -- manual / scheduled / ...
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running', -- running / succeeded / failed / paused-by-audit
    limit_param     INTEGER,
    git_sha         TEXT,
    clip_version    TEXT,
    sbert_version   TEXT,
    llm_model       TEXT,
    prompt_version  TEXT
);

-- ---------------------------------------------------------------------------
-- audit_results — queryable history of audit outcomes (§3.3 bonus).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- pipeline_metrics — health + data-quality metrics, keyed by run + name (§3.4).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id            BIGSERIAL PRIMARY KEY,
    run_id        UUID NOT NULL REFERENCES pipeline_runs(run_id),
    metric_name   TEXT NOT NULL,
    metric_value  DOUBLE PRECISION,
    metric_detail JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_pipeline_metrics_run_name UNIQUE (run_id, metric_name)
);

-- ---------------------------------------------------------------------------
-- Traceability columns on the existing destination tables.
-- Columns are nullable so `make migrate` succeeds on an already-populated DB;
-- the pipeline always writes them, and the audit/observability stages verify
-- that every row for a run has a non-null run_id.
-- ---------------------------------------------------------------------------
ALTER TABLE screens_metadata
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

ALTER TABLE screens_embeddings
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

ALTER TABLE screens_review_queue
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

ALTER TABLE screens_eval
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id);

-- ---------------------------------------------------------------------------
-- screens_embeddings: replace the composite primary key with a surrogate id.
--
-- 001 made (screen_id, model_name, model_version, embedding_kind) the PRIMARY
-- KEY. That makes the duplicate-detection audit (§3.3) impossible: the database
-- would reject the duplicate before the audit could ever see it, and the
-- Definition of Done explicitly requires manually inserting a duplicate to
-- prove the audit halts the pipeline. So the four-column tuple becomes an
-- unconstrained logical key that the audit task is responsible for policing.
--
-- Guarded by the absence of `id` so the whole swap happens exactly once.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'screens_embeddings'
          AND column_name = 'id'
    ) THEN
        ALTER TABLE screens_embeddings DROP CONSTRAINT IF EXISTS screens_embeddings_pkey;
        ALTER TABLE screens_embeddings ADD COLUMN id BIGSERIAL PRIMARY KEY;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Indexes for run-scoped queries and the audit's duplicate-detection GROUP BY.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_screens_metadata_run   ON screens_metadata (run_id);
CREATE INDEX IF NOT EXISTS ix_screens_embeddings_run ON screens_embeddings (run_id);
CREATE INDEX IF NOT EXISTS ix_screens_embeddings_key
    ON screens_embeddings (screen_id, model_name, model_version, embedding_kind);
CREATE INDEX IF NOT EXISTS ix_pipeline_metrics_run   ON pipeline_metrics (run_id, metric_name);
CREATE INDEX IF NOT EXISTS ix_audit_results_run      ON audit_results (run_id);
