"""Part C observability + notifications helpers.

This module owns:
  * Slack notifications (best effort, never fail the pipeline),
  * pipeline_metrics upserts,
  * end-of-run summary assembly (health + data quality).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from rico import config

log = logging.getLogger(__name__)

UPSERT_METRIC_SQL = """
INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, metric_detail)
VALUES (%s, %s, %s, %s::jsonb)
ON CONFLICT (run_id, metric_name) DO UPDATE SET
    metric_value  = EXCLUDED.metric_value,
    metric_detail = EXCLUDED.metric_detail,
    created_at    = NOW()
"""


def _upsert_metric(
    cur,
    *,
    run_id: str,
    name: str,
    value: float | None,
    detail: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> None:
    cur.execute(
        UPSERT_METRIC_SQL,
        (run_id, name, value, json.dumps(detail) if detail is not None else None),
    )


def _pct(num: int, den: int) -> float:
    if den <= 0:
        return 0.0
    return 100.0 * float(num) / float(den)


def _status_metric_value(status: str | None) -> float | None:
    mapping = {
        "succeeded": 1.0,
        "running": 0.5,
        "failed": 0.0,
        "paused-by-audit": -1.0,
    }
    if status is None:
        return None
    return mapping.get(status)


def _total_duration_seconds(started_at, ended_at) -> float | None:
    if not isinstance(started_at, datetime) or not isinstance(ended_at, datetime):
        return None
    return max(0.0, float((ended_at - started_at).total_seconds()))


def post_slack_message(text: str) -> bool:
    """Best-effort Slack post. Returns ``True`` on success, ``False`` otherwise."""
    url = (config.SLACK_WEBHOOK_URL or "").strip()
    if not url:
        return False

    try:
        import requests

        resp = requests.post(url, json={"text": text}, timeout=8)
        resp.raise_for_status()
        return True
    except Exception:
        log.exception("stage=notify slack_post_failed")
        return False


def notify_run_started(
    *,
    run_id: str,
    limit_param: int,
    triggered_by: str,
    dag_run_id: str,
) -> None:
    text = (
        "RICO pipeline run started\n"
        f"- run_id: `{run_id}`\n"
        f"- dag_run_id: `{dag_run_id}`\n"
        f"- limit: `{limit_param}`\n"
        f"- trigger: `{triggered_by}`"
    )
    post_slack_message(text)


def notify_audit_failed(
    *,
    run_id: str,
    details: dict[str, Any],
    log_url: str | None = None,
) -> None:
    details_json = json.dumps(details, indent=2, sort_keys=True)
    text = (
        "RICO pipeline audit failed\n"
        f"- run_id: `{run_id}`\n"
        f"- task_log: {log_url or 'n/a'}\n"
        "- duplicate keys:\n"
        f"```{details_json}```"
    )
    post_slack_message(text)


def notify_run_finished(
    *,
    run_id: str,
    status: str,
    total_duration_sec: float | None,
    summary_line: str,
) -> None:
    duration_txt = "n/a" if total_duration_sec is None else f"{total_duration_sec:.2f}s"
    text = (
        "RICO pipeline run finished\n"
        f"- run_id: `{run_id}`\n"
        f"- final_status: `{status}`\n"
        f"- total_duration: `{duration_txt}`\n"
        f"- summary: `{summary_line}`"
    )
    post_slack_message(text)


def collect_and_store_run_metrics(
    *,
    run_id: str,
    final_status: str | None,
    task_stats: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compute and persist health + data-quality metrics for a run."""
    from rico import db

    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT started_at, ended_at
            FROM pipeline_runs
            WHERE run_id = %s
            """,
            (run_id,),
        )
        started_at, ended_at = cur.fetchone() or (None, None)
        total_duration_sec = _total_duration_seconds(started_at, ended_at)

        cur.execute(
            """
            SELECT
                count(*)::int AS metadata_rows,
                count(extraction_payload)::int AS extraction_non_null_rows,
                count(*) FILTER (WHERE confidence >= 0.5)::int AS confidence_ge_05_rows,
                count(DISTINCT app_package)::int AS distinct_app_package,
                count(DISTINCT category)::int AS distinct_category
            FROM screens_metadata
            WHERE run_id = %s
            """,
            (run_id,),
        )
        (
            metadata_rows,
            extraction_non_null_rows,
            confidence_ge_05_rows,
            distinct_app_package,
            distinct_category,
        ) = cur.fetchone() or (0, 0, 0, 0, 0)

        cur.execute(
            "SELECT count(*)::int FROM screens_review_queue WHERE run_id = %s",
            (run_id,),
        )
        review_rows = (cur.fetchone() or [0])[0]

        cur.execute(
            """
            SELECT
                count(*)::int AS embeddings_rows,
                coalesce(avg(vector_dims(vector)), 0)::double precision AS avg_dims,
                count(DISTINCT vector_dims(vector))::int AS distinct_dims,
                count(*) FILTER (WHERE vector_norm(vector) = 0)::int AS zero_norm_rows
            FROM screens_embeddings
            WHERE run_id = %s
            """,
            (run_id,),
        )
        embeddings_rows, avg_dims, distinct_dims, zero_norm_rows = cur.fetchone() or (
            0,
            0.0,
            0,
            0,
        )

        cur.execute(
            """
            SELECT model_version, embedding_kind, count(*)::int AS n
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY model_version, embedding_kind
            ORDER BY model_version, embedding_kind
            """,
            (run_id,),
        )
        rows_by_model_kind = [
            {"model_version": mv, "embedding_kind": kind, "rows": n}
            for mv, kind, n in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT recall_at_5, n_queries
            FROM screens_eval
            WHERE run_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (run_id,),
        )
        eval_row = cur.fetchone()
        recall_at_5 = float(eval_row[0]) if eval_row else None
        eval_queries = int(eval_row[1]) if eval_row else 0

        extraction_non_null_pct = _pct(extraction_non_null_rows, metadata_rows)
        confidence_ge_05_pct = _pct(confidence_ge_05_rows, metadata_rows)
        review_queue_pct = _pct(review_rows, metadata_rows)
        zero_norm_pct = _pct(zero_norm_rows, embeddings_rows)

        # Health metrics.
        _upsert_metric(
            cur,
            run_id=run_id,
            name="health.final_status",
            value=_status_metric_value(final_status),
            detail={"status": final_status},
        )
        _upsert_metric(
            cur,
            run_id=run_id,
            name="health.total_run_duration_sec",
            value=total_duration_sec,
        )

        for task_id, stats in (task_stats or {}).items():
            duration = stats.get("duration_sec")
            retries = stats.get("retries")
            rows_in = stats.get("rows_in")
            rows_out = stats.get("rows_out")
            state = stats.get("state")
            _upsert_metric(
                cur,
                run_id=run_id,
                name=f"health.task_duration_sec.{task_id}",
                value=float(duration) if duration is not None else None,
            )
            _upsert_metric(
                cur,
                run_id=run_id,
                name=f"health.task_retries.{task_id}",
                value=float(retries) if retries is not None else None,
            )
            _upsert_metric(
                cur,
                run_id=run_id,
                name=f"health.task_rows_in.{task_id}",
                value=float(rows_in) if rows_in is not None else None,
            )
            _upsert_metric(
                cur,
                run_id=run_id,
                name=f"health.task_rows_out.{task_id}",
                value=float(rows_out) if rows_out is not None else None,
            )
            _upsert_metric(
                cur,
                run_id=run_id,
                name=f"health.task_state.{task_id}",
                value=_status_metric_value("succeeded" if state == "success" else state),
                detail={"state": state},
            )

        # Data-quality metrics.
        _upsert_metric(cur, run_id=run_id, name="dq.metadata.rows", value=float(metadata_rows))
        _upsert_metric(
            cur,
            run_id=run_id,
            name="dq.metadata.extraction_non_null_pct",
            value=extraction_non_null_pct,
        )
        _upsert_metric(
            cur,
            run_id=run_id,
            name="dq.metadata.confidence_ge_0_5_pct",
            value=confidence_ge_05_pct,
        )
        _upsert_metric(
            cur,
            run_id=run_id,
            name="dq.metadata.review_queue_pct",
            value=review_queue_pct,
        )
        _upsert_metric(
            cur,
            run_id=run_id,
            name="dq.metadata.distinct_app_package",
            value=float(distinct_app_package),
        )
        _upsert_metric(
            cur,
            run_id=run_id,
            name="dq.metadata.distinct_category",
            value=float(distinct_category),
        )

        _upsert_metric(
            cur, run_id=run_id, name="dq.embeddings.rows_total", value=float(embeddings_rows)
        )
        _upsert_metric(
            cur, run_id=run_id, name="dq.embeddings.avg_vector_dims", value=float(avg_dims)
        )
        _upsert_metric(
            cur,
            run_id=run_id,
            name="dq.embeddings.distinct_vector_dims",
            value=float(distinct_dims),
            detail={"warning": bool(distinct_dims > 1)},
        )
        _upsert_metric(
            cur,
            run_id=run_id,
            name="dq.embeddings.zero_norm_pct",
            value=zero_norm_pct,
        )
        _upsert_metric(
            cur,
            run_id=run_id,
            name="dq.embeddings.rows_by_model_kind",
            value=float(embeddings_rows),
            detail=rows_by_model_kind,
        )

        if recall_at_5 is not None:
            _upsert_metric(cur, run_id=run_id, name="dq.eval.recall_at_5", value=recall_at_5)
            _upsert_metric(
                cur,
                run_id=run_id,
                name="dq.eval.n_queries",
                value=float(eval_queries),
            )

        conn.commit()

    return {
        "run_id": run_id,
        "final_status": final_status or "unknown",
        "total_run_duration_sec": total_duration_sec,
        "metadata_rows": int(metadata_rows),
        "extraction_non_null_pct": extraction_non_null_pct,
        "confidence_ge_05_pct": confidence_ge_05_pct,
        "review_queue_pct": review_queue_pct,
        "embeddings_rows": int(embeddings_rows),
        "avg_vector_dims": float(avg_dims),
        "distinct_vector_dims": int(distinct_dims),
        "zero_norm_pct": zero_norm_pct,
        "recall_at_5": recall_at_5,
        "eval_queries": int(eval_queries),
    }


def format_summary_line(summary: dict[str, Any]) -> str:
    """One-screen-tall run summary for Airflow logs + Slack."""
    recall = summary.get("recall_at_5")
    recall_txt = "n/a" if recall is None else f"{float(recall):.3f}"
    duration = summary.get("total_run_duration_sec")
    duration_txt = "n/a" if duration is None else f"{float(duration):.2f}"
    return (
        f"status={summary.get('final_status')} duration_s={duration_txt} "
        f"metadata_rows={summary.get('metadata_rows', 0)} "
        f"extract_non_null_pct={float(summary.get('extraction_non_null_pct', 0.0)):.1f} "
        f"conf_ge_0_5_pct={float(summary.get('confidence_ge_05_pct', 0.0)):.1f} "
        f"review_queue_pct={float(summary.get('review_queue_pct', 0.0)):.1f} "
        f"embeddings_rows={summary.get('embeddings_rows', 0)} "
        f"zero_norm_pct={float(summary.get('zero_norm_pct', 0.0)):.1f} "
        f"recall_at_5={recall_txt}"
    )
