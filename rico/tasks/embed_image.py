"""embed_image stage - CLIP image embeddings staged in MinIO for ``load``.

Reference: Each screen's L2-normalized image vector is staged as a numpy ``.npz`` in MinIO
the ``load`` task is the one place that writes ``screens_embeddings``.
"""

import io
import logging

log = logging.getLogger(__name__)

SELECT_RUN_SCREENS = (
    "SELECT screen_id, png_path FROM screens_metadata "
    "WHERE run_id = %s ORDER BY screen_id"
)


def run_embed_image(run_id: str) -> dict:
    """CLIP-embed every PNG ingested by this run; stage vectors in MinIO."""
    import numpy as np
    import open_clip
    import torch
    from PIL import Image

    from rico import config, db, storage
    from rico.fingerprint import sha256_bytes

    log.info("run=%s stage=embed_image starting model=%s",
        run_id, config.CLIP_MODEL_VERSION,
    )

    clip_model, _preprocess_train, clip_preprocess = open_clip.create_model_and_transforms(
        config.CLIP_ARCH, pretrained=config.CLIP_PRETRAINED
    )
    clip_model.eval()

    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(SELECT_RUN_SCREENS, (run_id,))
        rows = cur.fetchall()

    if not rows:
        log.warning("run=%s stage=embed_image no screens to embed", run_id)
        return {"run_id": run_id, "embedded": 0}

    screen_ids: list[int] = []
    fingerprints: list[str] = []
    tensors: list = []
    for screen_id, png_path in rows:
        png_bytes = storage.get_bytes(png_path)
        fingerprints.append(sha256_bytes(png_bytes))
        img = Image.open(io.BytesIO(png_bytes)).convert("RGB")
        tensors.append(clip_preprocess(img))
        screen_ids.append(int(screen_id))

    images_tensor = torch.stack(tensors)
    with torch.no_grad():
        vectors = clip_model.encode_image(images_tensor)
        vectors = vectors / vectors.norm(dim=-1, keepdim=True)
    vectors_np = vectors.cpu().numpy().astype("float32")

    for screen_id, vec, fp in zip(screen_ids, vectors_np, fingerprints):
        buf = io.BytesIO()
        np.savez(buf, vector=vec, fingerprint=np.array(fp))
        storage.put_bytes(storage.clip_vector_key(screen_id), buf.getvalue())
        log.info(
            "run=%s stage=embed_image screen=%s dim=%d norm=%.4f fingerprint=%s",
            run_id, screen_id, vec.shape[0], float(np.linalg.norm(vec)), fp[:12],
        )

    log.info(
        "run=%s stage=embed_image complete screens=%d model=%s",
        run_id, len(screen_ids), config.CLIP_MODEL_VERSION,
    )
    return {"run_id": run_id, "embedded": len(screen_ids)}
