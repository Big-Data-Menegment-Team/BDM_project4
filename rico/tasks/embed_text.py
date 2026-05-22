"""embed_text stage - SBERT text embeddings staged in MinIO for ``load``.

The per-screen INSERT is removed, each L2-normalized text vector is staged
as a numpy ``.npz`` in MinIO (see :func:`rico.storage.sbert_vector_key`).
The ``load`` task is the one place that writes ``screens_embeddings``.

Input: the text representation written to ``screens/{id}.txt`` by ``parse``.
"""

import io
import logging

log = logging.getLogger(__name__)

SELECT_RUN_SCREENS = (
    "SELECT screen_id FROM screens_metadata "
    "WHERE run_id = %s ORDER BY screen_id"
)


def run_embed_text(run_id: str) -> dict:
    """SBERT-embed every text rep for this run; stage vectors in MinIO."""
    import numpy as np
    from sentence_transformers import SentenceTransformer

    from rico import config, db, storage
    from rico.fingerprint import sha256_text

    log.info(
        "run=%s stage=embed_text starting model=%s",
        run_id, config.SBERT_MODEL_VERSION,
    )

    sbert = SentenceTransformer(config.SBERT_MODEL_VERSION)

    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(SELECT_RUN_SCREENS, (run_id,))
        rows = cur.fetchall()

    if not rows:
        log.warning("run=%s stage=embed_text no screens to embed", run_id)
        return {"run_id": run_id, "embedded": 0}

    screen_ids: list[int] = []
    text_reps: list[str] = []
    fingerprints: list[str] = []
    for (screen_id,) in rows:
        text = storage.get_bytes(storage.text_key(screen_id)).decode("utf-8")
        screen_ids.append(int(screen_id))
        text_reps.append(text)
        fingerprints.append(sha256_text(text))

    vectors_np = sbert.encode(text_reps, normalize_embeddings=True).astype("float32")

    for screen_id, vec, fp in zip(screen_ids, vectors_np, fingerprints):
        buf = io.BytesIO()
        np.savez(buf, vector=vec, fingerprint=np.array(fp))
        storage.put_bytes(storage.sbert_vector_key(screen_id), buf.getvalue())
        log.info(
            "run=%s stage=embed_text screen=%s dim=%d norm=%.4f fingerprint=%s",
            run_id, screen_id, vec.shape[0], float(np.linalg.norm(vec)), fp[:12],
        )

    log.info(
        "run=%s stage=embed_text complete screens=%d model=%s",
        run_id, len(screen_ids), config.SBERT_MODEL_VERSION,
    )
    return {"run_id": run_id, "embedded": len(screen_ids)}
