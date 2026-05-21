"""embed_text stage — PART B (stub, not yet implemented).

Owner: Part B. Reference: Section 4 of the lab notebook. See TASK_SPLIT.md.
"""

import logging

log = logging.getLogger(__name__)


def run_embed_text(run_id: str):
    """SBERT-embed each screen's text representation; stage vectors for load.

    Contract for Part B:
      * Find this run's screens with ``screens_metadata WHERE run_id = %s``.
      * Read each text rep from MinIO via ``rico.storage.get_bytes`` on
        ``rico.storage.text_key(screen_id)`` (written by the parse stage).
      * Encode with SBERT ``all-MiniLM-L6-v2`` (``rico.config.SBERT_MODEL_VERSION``);
        normalize embeddings.
      * ``embedding_kind='text'``, ``model_name='sentence-transformers'``.
      * ``source_fingerprint = rico.fingerprint.sha256_text(<text rep>)``.
      * Hand vectors to ``load`` (the staging mechanism is Part B's choice).
    """
    raise NotImplementedError(
        "embed_text is owned by Part B — see TASK_SPLIT.md (Part B)"
    )
