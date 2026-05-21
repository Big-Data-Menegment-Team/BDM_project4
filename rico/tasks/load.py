"""load stage — PART B (stub, not yet implemented).

Owner: Part B. See TASK_SPLIT.md. This stage owns idempotency.
"""

import logging

log = logging.getLogger(__name__)


def run_load(run_id: str):
    """Persist this run's embeddings, extractions and review-queue rows.

    Contract for Part B:
      * Write whatever ``embed_image`` / ``embed_text`` / ``extract`` staged.
      * Every destination row must carry a non-null ``run_id`` and
        ``source_fingerprint``.
      * Idempotency — re-running a run must create no new rows:
          - ``screens_metadata``: ``INSERT ... ON CONFLICT (screen_id) DO UPDATE``.
          - ``screens_embeddings``: it has NO unique constraint on
            ``(screen_id, model_name, model_version, embedding_kind)`` — that is
            deliberate, so the audit can detect duplicates (see
            ``migrations/002_traceability.sql``). Use a scoped
            delete-then-insert (delete this run's target rows, then insert).
          - ``screens_review_queue``: de-duplicate per run as well.
    """
    raise NotImplementedError(
        "load is owned by Part B — see TASK_SPLIT.md (Part B)"
    )
