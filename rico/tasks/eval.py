"""eval stage — PART C (stub, not yet implemented).

Owner: Part C. Reference: Section 7 of the lab notebook, README §4. See TASK_SPLIT.md.
"""

import logging

log = logging.getLogger(__name__)


def run_eval(run_id: str):
    """Compute retrieval quality (recall@5) for this run.

    Contract for Part C:
      * recall@5 with a self-test holdout is acceptable; a disjoint holdout
        (notebook Section 7) is the optional stretch.
      * Write a ``screens_eval`` row carrying this run's ``run_id``.
      * recall@5 also feeds the end-of-run data-quality summary (§3.4).
    """
    raise NotImplementedError(
        "eval is owned by Part C — see TASK_SPLIT.md (Part C)"
    )
