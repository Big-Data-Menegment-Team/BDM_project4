"""load stage - idempotent writes from MinIO-staged artifacts into Postgres.

This is the one place that mutates ``screens_embeddings``,
``screens_metadata.extraction_payload`` and ``screens_review_queue``.
Idempotency logic here:

``screens_embeddings`` has no unique constraint on ``(screen_id, model_name, model_version, embedding_kind)`` - the audit
polices that tuple (see ``migrations/002_traceability.sql``). To stay
idempotent we **delete the rows for this run's screens first, then insert**
the staged vectors. The delete is scoped by *screen_id* (the natural key),
NOT by run_id, each Airflow re-trigger gets a fresh dag_run_id (and thus a
fresh run_id), so a run_id-scoped delete would leave the previous run's rows
behind and cause the table to grow on every re-trigger.

``screens_review_queue`` has no natural unique key - same screen-scoped
delete-then-insert.

``screens_metadata`` is updated in place: ingest already wrote the row
(PK ``screen_id``), so load only stamps the extraction columns.

Everything runs in a single transaction, so a failure leaves the runs
prior data untouched.
"""

import io
import json
import logging

log = logging.getLogger(__name__)

SELECT_RUN_SCREENS = (
    "SELECT screen_id FROM screens_metadata "
    "WHERE run_id = %s ORDER BY screen_id"
)

DELETE_EMBEDDINGS_FOR_SCREENS = (
    "DELETE FROM screens_embeddings WHERE screen_id = ANY(%s)"
)

INSERT_EMBEDDING = """
INSERT INTO screens_embeddings
    (screen_id, model_name, model_version, embedding_kind,
     vector, run_id, source_fingerprint)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

DELETE_REVIEW_QUEUE_FOR_SCREENS = (
    "DELETE FROM screens_review_queue WHERE screen_id = ANY(%s)"
)

INSERT_REVIEW_QUEUE = """
INSERT INTO screens_review_queue
    (screen_id, reason, raw_output, run_id, source_fingerprint)
VALUES (%s, %s, %s, %s, %s)
"""

UPDATE_METADATA_EXTRACTION = """
UPDATE screens_metadata
SET extraction_payload = %s::jsonb,
    prompt_version     = %s,
    confidence         = %s,
    run_id             = %s,
    source_fingerprint = %s,
    updated_at         = NOW()
WHERE screen_id = %s
"""


def _load_npz_artifact(key: str) -> tuple["object | None", str | None]:
    """Read a staged ``.npz`` from MinIO; return (vector_np, fingerprint) or (None, None) if absent."""
    import numpy as np

    from rico import storage

    if not storage.object_exists(key):
        return None, None
    data = storage.get_bytes(key)
    with np.load(io.BytesIO(data)) as archive:
        vector = archive["vector"].astype("float32")
        fingerprint = str(archive["fingerprint"])
    return vector, fingerprint


def _read_extraction_artifact(screen_id: int) -> dict | None:
    """Read the staged ``screens/{id}.extract.json``; return ``None`` if absent."""
    from rico import storage

    key = storage.extraction_key(screen_id)
    if not storage.object_exists(key):
        return None
    return json.loads(storage.get_bytes(key).decode("utf-8"))


def _coerce_confidence(payload: dict) -> float | None:
    """Pull ``confidence`` out of the LLM payload as a float in [0, 1] (or ``None``)."""
    value = payload.get("confidence") if isinstance(payload, dict) else None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def run_load(run_id: str) -> dict:
    """Persist this run's embeddings, extractions and review-queue rows."""
    from rico import config, db, storage
    from rico.fingerprint import sha256_text

    with db.connection(register_pgvector=True) as conn, conn.cursor() as cur:
        cur.execute(SELECT_RUN_SCREENS, (run_id,))
        screen_ids = [int(sid) for (sid,) in cur.fetchall()]

        if not screen_ids:
            log.warning("run=%s stage=load no screens to load", run_id)
            return {"run_id": run_id, "screens": 0}

        cur.execute(DELETE_EMBEDDINGS_FOR_SCREENS, (screen_ids,))
        deleted_embeddings = cur.rowcount

        image_inserted = 0
        text_inserted = 0
        for screen_id in screen_ids:
            clip_vec, clip_fp = _load_npz_artifact(storage.clip_vector_key(screen_id))
            if clip_vec is None:
                log.error(
                    "run=%s stage=load screen=%s missing clip vector; skipping image row",
                    run_id, screen_id,
                )
            else:
                cur.execute(
                    INSERT_EMBEDDING,
                    (
                        screen_id,
                        "open-clip",
                        config.CLIP_MODEL_VERSION,
                        "image",
                        clip_vec,
                        run_id,
                        clip_fp,
                    ),
                )
                image_inserted += 1

            sbert_vec, sbert_fp = _load_npz_artifact(storage.sbert_vector_key(screen_id))
            if sbert_vec is None:
                log.error(
                    "run=%s stage=load screen=%s missing sbert vector; skipping text row",
                    run_id, screen_id,
                )
            else:
                cur.execute(
                    INSERT_EMBEDDING,
                    (
                        screen_id,
                        "sentence-transformers",
                        config.SBERT_MODEL_VERSION,
                        "text",
                        sbert_vec,
                        run_id,
                        sbert_fp,
                    ),
                )
                text_inserted += 1

        cur.execute(DELETE_REVIEW_QUEUE_FOR_SCREENS, (screen_ids,))
        deleted_review = cur.rowcount

        extracted_updates = 0
        review_inserts = 0
        for screen_id in screen_ids:
            artifact = _read_extraction_artifact(screen_id)

            if artifact is None:
                # Extract task never produced an artifact for this screen.
                text = storage.get_bytes(storage.text_key(screen_id)).decode("utf-8")
                cur.execute(
                    INSERT_REVIEW_QUEUE,
                    (screen_id, "extract_missing", None, run_id, sha256_text(text)),
                )
                review_inserts += 1
                log.warning(
                    "run=%s stage=load screen=%s extract_missing -> review_queue",
                    run_id, screen_id,
                )
                continue

            fingerprint = artifact.get("fingerprint")
            if not artifact.get("ok"):
                cur.execute(
                    INSERT_REVIEW_QUEUE,
                    (
                        screen_id,
                        artifact.get("error") or "extract_failed",
                        artifact.get("raw"),
                        run_id,
                        fingerprint,
                    ),
                )
                review_inserts += 1
                log.warning(
                    "run=%s stage=load screen=%s extract_failed -> review_queue reason=%r",
                    run_id, screen_id, artifact.get("error"),
                )
                continue

            payload = artifact.get("payload") or {}
            cur.execute(
                UPDATE_METADATA_EXTRACTION,
                (
                    json.dumps(payload),
                    config.PROMPT_VERSION,
                    _coerce_confidence(payload),
                    run_id,
                    fingerprint,
                    screen_id,
                ),
            )
            extracted_updates += 1

        conn.commit()

    log.info(
        "run=%s stage=load complete screens=%d "
        "embeddings deleted=%d image_inserted=%d text_inserted=%d "
        "review deleted=%d review_inserted=%d extraction_updated=%d",
        run_id, len(screen_ids),
        deleted_embeddings, image_inserted, text_inserted,
        deleted_review, review_inserts, extracted_updates,
    )
    return {
        "run_id": run_id,
        "screens": len(screen_ids),
        "image_inserted": image_inserted,
        "text_inserted": text_inserted,
        "review_inserted": review_inserts,
        "extraction_updated": extracted_updates,
    }
