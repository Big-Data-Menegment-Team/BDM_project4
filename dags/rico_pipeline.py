"""RICO production pipeline — Airflow DAG.

Re-implements the lab notebook's pipeline as a scheduled, idempotent, observable,
auditable DAG:

    init_run -> ingest -> parse -> [embed_image, embed_text, extract]
             -> load -> audit -> eval

This file is orchestration only — every task delegates to a function in the
``rico`` package. Heavy imports are deferred into the task bodies so DAG parsing
stays fast. See ``TASK_SPLIT.md`` and ``README.md`` for the full design.

Note: the task parameter is ``pipeline_run_id`` (our pipeline_runs UUID), NOT
``run_id`` — ``run_id`` is a reserved Airflow task-context key and cannot be a
TaskFlow parameter name.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

DEFAULT_LIMIT = 5


def _finalize_callback(success: bool):
    """Build a DAG-level callback that stamps the pipeline_runs row.

    Observability must never crash the run, so any failure here is swallowed.
    """

    def _callback(context):
        dag_run = context["dag_run"]
        try:
            from rico import db

            db.finalize_pipeline_run(dag_run.run_id, success=success)
        except Exception:
            logging.getLogger(__name__).exception(
                "could not finalize pipeline_runs row for dag_run=%s", dag_run.run_id
            )

    return _callback


@dag(
    dag_id="rico_pipeline",
    description="RICO multimodal pipeline — ingest, embed, extract, load, audit, eval.",
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

        from rico import db

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
            pipeline_run_id, dag_run.run_id, limit, dag_run.run_type,
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

    # --- wiring -------------------------------------------------------------
    # Every task takes pipeline_run_id, so all are data-dependent on init_run.
    # The `>>` chain adds stage ordering, including the embed/extract fan-out.
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
