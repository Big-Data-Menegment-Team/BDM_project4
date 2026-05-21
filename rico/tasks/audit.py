"""audit stage — PART C (stub, not yet implemented).

Owner: Part C. Reference: README §3.3. See TASK_SPLIT.md.
"""

import logging

log = logging.getLogger(__name__)


def run_audit(run_id: str):
    """Duplicate-detection audit — the pipeline's circuit breaker.

    Contract for Part C:
      * Check that no ``(screen_id, model_name, model_version, embedding_kind)``
        combination appears more than once in ``screens_embeddings``, and no
        ``screen_id`` appears more than once in ``screens_metadata`` for this run.
      * Log every duplicate key found, in full.
      * Record the outcome in ``audit_results`` (run_id, audit_name, passed, details).
      * On failure: set ``pipeline_runs.status = 'paused-by-audit'`` for this run,
        then raise (e.g. ``AirflowException``) so ``eval`` is skipped and the run
        halts. The DAG's finalize callback preserves the ``paused-by-audit`` status.
    """
    raise NotImplementedError(
        "audit is owned by Part C — see TASK_SPLIT.md (Part C)"
    )
