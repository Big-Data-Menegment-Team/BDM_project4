"""RICO production pipeline — Airflow DAG.

Re-implements the lab notebook's pipeline as a scheduled, idempotent, observable,
auditable DAG:

    init_run -> ingest -> parse -> [embed_image, embed_text, extract]
             -> load -> audit -> eval

This file is orchestration only — every task delegates to a function in the
``rico`` package. Heavy imports are deferred into the task bodies so DAG parsing
stays fast. See ``TASK_SPLIT.md`` and ``README.md`` for the full design.
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
        run_id = db.create_pipeline_run(
            dag_run_id=dag_run.run_id,
            triggered_by=str(dag_run.run_type),
            limit_param=limit,
        )
        log.info(
            "run=%s stage=init_run dag_run=%s limit=%d trigger=%s",
            run_id, dag_run.run_id, limit, dag_run.run_type,
        )
        return run_id

    @task
    def ingest(run_id: str) -> list[int]:
        """Stream the first LIMIT screens into MinIO + screens_metadata."""
        from airflow.operators.python import get_current_context

        from rico.tasks.ingest import run_ingest

        limit = int(get_current_context()["params"]["limit"])
        return run_ingest(run_id, limit)

    @task
    def parse(run_id: str) -> dict:
        """Parse view hierarchies into text representations in MinIO."""
        from rico.tasks.parse import run_parse

        return run_parse(run_id)

    @task
    def embed_image(run_id: str):
        from rico.tasks.embed_image import run_embed_image

        return run_embed_image(run_id)

    @task
    def embed_text(run_id: str):
        from rico.tasks.embed_text import run_embed_text

        return run_embed_text(run_id)

    @task
    def extract(run_id: str):
        from rico.tasks.extract import run_extract

        return run_extract(run_id)

    @task
    def load(run_id: str):
        from rico.tasks.load import run_load

        return run_load(run_id)

    @task
    def audit(run_id: str):
        from rico.tasks.audit import run_audit

        return run_audit(run_id)

    @task(task_id="eval")
    def evaluate(run_id: str):
        from rico.tasks.eval import run_eval

        return run_eval(run_id)

    # --- wiring -------------------------------------------------------------
    # Every task takes run_id, so all are data-dependent on init_run. The `>>`
    # chain adds the stage ordering, including the parallel embed/extract fan-out.
    run_id = init_run()
    ingested = ingest(run_id)
    parsed = parse(run_id)
    image_vectors = embed_image(run_id)
    text_vectors = embed_text(run_id)
    extracted = extract(run_id)
    loaded = load(run_id)
    audited = audit(run_id)
    evaluated = evaluate(run_id)

    (
        ingested
        >> parsed
        >> [image_vectors, text_vectors, extracted]
        >> loaded
        >> audited
        >> evaluated
    )


rico_pipeline()
