"""Postgres helpers: connections and the pipeline_runs lifecycle.

`pipeline_runs` is the anchor of record traceability (§3.2): one row per DAG
run, created by the `init_run` task and finalized by the DAG-level callbacks.
"""

import logging
import os
import subprocess
import uuid
from typing import Any

import psycopg

from rico import config

log = logging.getLogger(__name__)


def connection(register_pgvector: bool = False):
    """Open a new psycopg connection to the rico database.

    Pass ``register_pgvector=True`` (used by the embedding stages) to enable
    passing ``numpy``/``list`` vectors straight to ``vector`` columns.
    """
    conn = psycopg.connect(config.POSTGRES_DSN)
    if register_pgvector:
        from pgvector.psycopg import register_vector

        register_vector(conn)
    return conn


def git_sha() -> str:
    """Return the git commit the pipeline code is running from.

    Read from the ``GIT_SHA`` env var (set by the Makefile and passed into the
    containers) since the ``.git`` directory is not mounted into the image.
    Falls back to a host ``git`` call, then to ``"unknown"``.
    """
    sha = os.getenv("GIT_SHA")
    if sha:
        return sha
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except Exception:  # pragma: no cover - git unavailable inside the container
        pass
    return "unknown"


def create_pipeline_run(dag_run_id: str, triggered_by: str, limit_param: int) -> str:
    """Insert a ``pipeline_runs`` row (status ``running``) and return its run_id.

    Keyed on ``dag_run_id``: if the ``init_run`` task is retried, the existing
    run_id is reused (the row is reset to ``running``) rather than duplicated.
    """
    new_run_id = str(uuid.uuid4())
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs
                (run_id, dag_run_id, triggered_by, status, limit_param, git_sha,
                 clip_version, sbert_version, llm_model, prompt_version)
            VALUES (%s, %s, %s, 'running', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dag_run_id) DO UPDATE SET
                triggered_by   = EXCLUDED.triggered_by,
                started_at     = NOW(),
                ended_at       = NULL,
                status         = 'running',
                limit_param    = EXCLUDED.limit_param,
                git_sha        = EXCLUDED.git_sha,
                clip_version   = EXCLUDED.clip_version,
                sbert_version  = EXCLUDED.sbert_version,
                llm_model      = EXCLUDED.llm_model,
                prompt_version = EXCLUDED.prompt_version
            RETURNING run_id
            """,
            (
                new_run_id,
                dag_run_id,
                triggered_by,
                limit_param,
                git_sha(),
                config.CLIP_MODEL_VERSION,
                config.SBERT_MODEL_VERSION,
                config.LLM_MODEL,
                config.PROMPT_VERSION,
            ),
        )
        run_id = cur.fetchone()[0]
        conn.commit()
    return str(run_id)


def finalize_pipeline_run(dag_run_id: str, success: bool) -> None:
    """Stamp ``ended_at`` and the final status on the run.

    On failure, only a run still marked ``running`` becomes ``failed`` — a
    status the audit task already set (e.g. ``paused-by-audit``) is preserved.
    """
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET ended_at = NOW(),
                status = CASE
                    WHEN %s THEN 'succeeded'
                    WHEN status = 'running' THEN 'failed'
                    ELSE status
                END
            WHERE dag_run_id = %s
            """,
            (success, dag_run_id),
        )
        conn.commit()


def set_pipeline_run_status(run_id: str, status: str) -> None:
    """Set ``pipeline_runs.status`` for a run_id."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET status = %s WHERE run_id = %s",
            (status, run_id),
        )
        conn.commit()


def get_pipeline_run_by_dag_run_id(dag_run_id: str) -> dict[str, Any] | None:
    """Return one ``pipeline_runs`` row as a dict (looked up by dag_run_id)."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, dag_run_id, triggered_by, started_at, ended_at, status,
                   limit_param, git_sha, clip_version, sbert_version, llm_model, prompt_version
            FROM pipeline_runs
            WHERE dag_run_id = %s
            """,
            (dag_run_id,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    keys = (
        "run_id",
        "dag_run_id",
        "triggered_by",
        "started_at",
        "ended_at",
        "status",
        "limit_param",
        "git_sha",
        "clip_version",
        "sbert_version",
        "llm_model",
        "prompt_version",
    )
    return {k: v for k, v in zip(keys, row)}


def get_pipeline_run(run_id: str) -> dict[str, Any] | None:
    """Return one ``pipeline_runs`` row as a dict (looked up by run_id)."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, dag_run_id, triggered_by, started_at, ended_at, status,
                   limit_param, git_sha, clip_version, sbert_version, llm_model, prompt_version
            FROM pipeline_runs
            WHERE run_id = %s
            """,
            (run_id,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    keys = (
        "run_id",
        "dag_run_id",
        "triggered_by",
        "started_at",
        "ended_at",
        "status",
        "limit_param",
        "git_sha",
        "clip_version",
        "sbert_version",
        "llm_model",
        "prompt_version",
    )
    return {k: v for k, v in zip(keys, row)}
