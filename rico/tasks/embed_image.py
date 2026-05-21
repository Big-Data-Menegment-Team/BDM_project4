"""embed_image stage — PART B (stub, not yet implemented).

Owner: Part B. Reference: Section 3 of the lab notebook. See TASK_SPLIT.md.
"""

import logging

log = logging.getLogger(__name__)


def run_embed_image(run_id: str):
    """CLIP-embed each screen's PNG and stage image vectors for the load stage.

    Contract for Part B:
      * Find this run's screens with
        ``SELECT screen_id, png_path FROM screens_metadata WHERE run_id = %s``.
      * Fetch PNG bytes from MinIO via ``rico.storage.get_bytes``.
      * Encode with open-clip ``ViT-B-32`` (``rico.config.CLIP_MODEL_VERSION``);
        L2-normalize each vector.
      * ``embedding_kind='image'``, ``model_name='open-clip'``.
      * ``source_fingerprint = rico.fingerprint.sha256_bytes(<exact PNG bytes>)``.
      * Hand vectors to ``load`` (the staging mechanism is Part B's choice).
    """
    raise NotImplementedError(
        "embed_image is owned by Part B — see TASK_SPLIT.md (Part B)"
    )
