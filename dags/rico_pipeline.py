"""RICO production pipeline - Airflow DAG orchestration only.

Pipeline shape:
    init_run -> ingest -> parse -> [embed_image, embed_text, extract]
             -> load -> audit -> eval

Part C adds the audit/eval task bodies plus end-of-run observability metrics and
Slack notifications.
"""

from __future__ import annotations

import logging
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

DEFAULT_LIMIT = 5


def _rows_out_from_xcom(task_id: str, payload: Any) -> int | None:
    if task_id == "init_run" and isinstance(payload, str):
        return 1
    if task_id == "ingest" and isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("screens", "screens_parsed", "embedded", "extracted", "n_queries"):
            value = payload.get(key)
            if isinstance(value, int):
                return value
    return None


def _collect_task_stats(context) -> dict[str, dict[str, Any]]:
    """Extract per-task health stats from the completed DAG run."""
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    stats: dict[str, dict[str, Any]] = {}

    for ti in task_instances:
        task_id = ti.task_id
        entry: dict[str, Any] = {
            "duration_sec": float(ti.duration or 0.0),
            "retries": max(int((ti.try_number or 1) - 1), 0),
            "state": str(ti.state or "unknown"),
        }
        try:
            payload = ti.xcom_pull(task_ids=task_id, key="return_value")
        except Exception:
            payload = None
        rows_out = _rows_out_from_xcom(task_id, payload)
        if rows_out is not None:
            entry["rows_out"] = rows_out
        stats[task_id] = entry

    ingest_out = stats.get("ingest", {}).get("rows_out")
    parse_out = stats.get("parse", {}).get("rows_out", ingest_out)
    load_out = stats.get("load", {}).get("rows_out")

    if "parse" in stats and ingest_out is not None:
        stats["parse"]["rows_in"] = ingest_out
    for task_id in ("embed_image", "embed_text", "extract", "load"):
        if task_id in stats and parse_out is not None:
            stats[task_id]["rows_in"] = parse_out
    if "audit" in stats and load_out is not None:
        stats["audit"]["rows_in"] = load_out
    if "eval" in stats and parse_out is not None:
        stats["eval"]["rows_in"] = parse_out

    return stats


def _finalize_callback(success: bool):
    """Build a DAG-level callback that finalizes run status + observability."""

    def _callback(context):
        dag_run = context["dag_run"]
        try:
            from rico import db, observability

            db.finalize_pipeline_run(dag_run.run_id, success=success)
            run_row = db.get_pipeline_run_by_dag_run_id(dag_run.run_id)
            if run_row is None:
                log.error("stage=finalize dag_run=%s run_row_missing=true", dag_run.run_id)
                return

            run_id = str(run_row["run_id"])
            final_status = str(run_row["status"])
            summary = observability.collect_and_store_run_metrics(
                run_id=run_id,
                final_status=final_status,
                task_stats=_collect_task_stats(context),
            )
            summary_line = observability.format_summary_line(summary)
            log.info("run=%s stage=summary %s", run_id, summary_line)
            observability.notify_run_finished(
                run_id=run_id,
                status=final_status,
                total_duration_sec=summary.get("total_run_duration_sec"),
                summary_line=summary_line,
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "could not finalize/observe dag_run=%s", dag_run.run_id
            )

    return _callback


@dag(
    dag_id="rico_pipeline",
    description="RICO multimodal pipeline - ingest, embed, extract, load, audit, eval.",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["rico", "bdm-project4"],
    params={
        "limit": Param(
            DEFAULT_LIMIT,
            type="integer",
            minimum=1,
            description="How many screens to process (dev: 5, demo: 50).",
        )
    },
    on_success_callback=_finalize_callback(True),
    on_failure_callback=_finalize_callback(False),
    doc_md=__doc__,
)
def rico_pipeline():
    @task
    def init_run() -> str:
        """Create the pipeline_runs row; return its run_id (UUID) for every task."""
        from airflow.operators.python import get_current_context

        from rico import db, observability

        context = get_current_context()
        dag_run = context["dag_run"]
        limit = int(context["params"]["limit"])
        pipeline_run_id = db.create_pipeline_run(
            dag_run_id=dag_run.run_id,
            triggered_by=str(dag_run.run_type),
            limit_param=limit,
        )
        log.info(
            "run=%s stage=init_run dag_run=%s limit=%d trigger=%s",
            pipeline_run_id,
            dag_run.run_id,
            limit,
            dag_run.run_type,
        )
        observability.notify_run_started(
            run_id=pipeline_run_id,
            limit_param=limit,
            triggered_by=str(dag_run.run_type),
            dag_run_id=dag_run.run_id,
        )
        return pipeline_run_id

    @task
    def ingest(pipeline_run_id: str) -> list[int]:
        """Stream the first LIMIT screens into MinIO + screens_metadata."""
        from airflow.operators.python import get_current_context

        from rico.tasks.ingest import run_ingest

        limit = int(get_current_context()["params"]["limit"])
        return run_ingest(pipeline_run_id, limit)

    @task
    def parse(pipeline_run_id: str) -> dict:
        """Parse view hierarchies into text representations in MinIO."""
        from rico.tasks.parse import run_parse

        return run_parse(pipeline_run_id)

    @task
    def embed_image(pipeline_run_id: str):
        from rico.tasks.embed_image import run_embed_image

        return run_embed_image(pipeline_run_id)

    @task
    def embed_text(pipeline_run_id: str):
        from rico.tasks.embed_text import run_embed_text

        return run_embed_text(pipeline_run_id)

    @task
    def extract(pipeline_run_id: str):
        from rico.tasks.extract import run_extract

        return run_extract(pipeline_run_id)

    @task
    def load(pipeline_run_id: str):
        from rico.tasks.load import run_load

        return run_load(pipeline_run_id)

    @task
    def audit(pipeline_run_id: str):
        from rico.tasks.audit import run_audit

        return run_audit(pipeline_run_id)

    @task(task_id="eval")
    def evaluate(pipeline_run_id: str):
        from rico.tasks.eval import run_eval

        return run_eval(pipeline_run_id)

    pipeline_run_id = init_run()
    ingested = ingest(pipeline_run_id)
    parsed = parse(pipeline_run_id)
    image_vectors = embed_image(pipeline_run_id)
    text_vectors = embed_text(pipeline_run_id)
    extracted = extract(pipeline_run_id)
    loaded = load(pipeline_run_id)
    audited = audit(pipeline_run_id)
    evaluated = evaluate(pipeline_run_id)

    (
        ingested
        >> parsed
        >> [image_vectors, text_vectors, extracted]
        >> loaded
        >> audited
        >> evaluated
    )


rico_pipeline()
