"""audit stage - duplicate-detection circuit breaker (Part C)."""

from __future__ import annotations

import json
import logging

log = logging.getLogger(__name__)

AUDIT_NAME = "duplicate_detection"

SELECT_EMBEDDING_DUPLICATES = """
SELECT
    screen_id,
    model_name,
    model_version,
    embedding_kind,
    count(*)::int AS dup_count
FROM screens_embeddings
GROUP BY screen_id, model_name, model_version, embedding_kind
HAVING count(*) > 1
ORDER BY dup_count DESC, screen_id, model_name, model_version, embedding_kind
"""

SELECT_METADATA_DUPLICATES_FOR_RUN = """
SELECT
    screen_id,
    count(*)::int AS dup_count
FROM screens_metadata
WHERE run_id = %s
GROUP BY screen_id
HAVING count(*) > 1
ORDER BY dup_count DESC, screen_id
"""

INSERT_AUDIT_RESULT = """
INSERT INTO audit_results (run_id, audit_name, passed, details)
VALUES (%s, %s, %s, %s::jsonb)
"""


def _task_log_url() -> str | None:
    """Best-effort fetch of the current task log URL from Airflow context."""
    try:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        ti = context.get("task_instance")
        return getattr(ti, "log_url", None)
    except Exception:
        return None


def run_audit(run_id: str) -> dict:
    """Run duplicate checks and fail loudly when broken data is detected."""
    from rico import db, observability

    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(SELECT_EMBEDDING_DUPLICATES)
        embedding_dupes = [
            {
                "screen_id": int(screen_id),
                "model_name": model_name,
                "model_version": model_version,
                "embedding_kind": embedding_kind,
                "count": int(dup_count),
            }
            for screen_id, model_name, model_version, embedding_kind, dup_count in cur.fetchall()
        ]

        cur.execute(SELECT_METADATA_DUPLICATES_FOR_RUN, (run_id,))
        metadata_dupes = [
            {"screen_id": int(screen_id), "count": int(dup_count)}
            for screen_id, dup_count in cur.fetchall()
        ]

        passed = not embedding_dupes and not metadata_dupes
        details = {
            "run_id": run_id,
            "embedding_duplicates": embedding_dupes,
            "metadata_duplicates_for_run": metadata_dupes,
        }

        cur.execute(
            INSERT_AUDIT_RESULT,
            (run_id, AUDIT_NAME, passed, json.dumps(details)),
        )

        if passed:
            conn.commit()
            log.info(
                "run=%s stage=audit passed=true embedding_duplicates=0 metadata_duplicates=0",
                run_id,
            )
            return {
                "run_id": run_id,
                "audit_name": AUDIT_NAME,
                "passed": True,
                "embedding_duplicates": 0,
                "metadata_duplicates": 0,
            }

        # Circuit breaker: mark run paused-by-audit and fail the task.
        cur.execute(
            "UPDATE pipeline_runs SET status = 'paused-by-audit' WHERE run_id = %s",
            (run_id,),
        )
        conn.commit()

    # Log every duplicate key in full.
    for row in embedding_dupes:
        log.error("run=%s stage=audit duplicate_embedding=%s", run_id, row)
    for row in metadata_dupes:
        log.error("run=%s stage=audit duplicate_metadata=%s", run_id, row)

    observability.notify_audit_failed(
        run_id=run_id,
        details=details,
        log_url=_task_log_url(),
    )

    from airflow.exceptions import AirflowException

    raise AirflowException(
        "duplicate-detection audit failed; run marked paused-by-audit and eval halted"
    )
