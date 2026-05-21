"""ingest stage — stream screens from HuggingFace into MinIO + Postgres.

Translated from Section 1 of the lab notebook, with two production changes:
a ``LIMIT`` parameter replaces the fixed ``chosen_screens.txt``, and the
INSERT is idempotent (``ON CONFLICT``) so re-running a run creates no new rows.
"""

import io
import itertools
import logging

log = logging.getLogger(__name__)

# Idempotent upsert: re-running with the same LIMIT updates rows in place
# (new run_id, new fingerprint) instead of raising a primary-key violation.
INSERT_METADATA_SQL = """
INSERT INTO screens_metadata
    (screen_id, app_package, category, png_path, hierarchy_json_path,
     run_id, source_fingerprint)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (screen_id) DO UPDATE SET
    app_package         = EXCLUDED.app_package,
    category            = EXCLUDED.category,
    png_path            = EXCLUDED.png_path,
    hierarchy_json_path = EXCLUDED.hierarchy_json_path,
    run_id              = EXCLUDED.run_id,
    source_fingerprint  = EXCLUDED.source_fingerprint,
    updated_at          = NOW()
"""


def run_ingest(run_id: str, limit: int) -> list[int]:
    """Ingest the first ``limit`` screens of the RICO dataset.

    For each screen: PUT the PNG and hierarchy JSON to MinIO, then upsert a
    ``screens_metadata`` row tagged with ``run_id`` and a ``source_fingerprint``
    (SHA-256 of the PNG bytes). Returns the list of ingested screen IDs.
    """
    from datasets import load_dataset

    from rico import config, db, storage
    from rico.fingerprint import sha256_bytes

    log.info(
        "run=%s stage=ingest starting limit=%d dataset=%s",
        run_id, limit, config.HF_DATASET,
    )
    dataset = load_dataset(
        config.HF_DATASET, split="train", streaming=True, trust_remote_code=True
    )

    screen_ids: list[int] = []
    with db.connection() as conn, conn.cursor() as cur:
        for row in itertools.islice(dataset, limit):
            screen_id = int(row["screenId"])

            png_buf = io.BytesIO()
            row["image"].save(png_buf, format="PNG")
            png_bytes = png_buf.getvalue()
            hierarchy_bytes = row["view_hierarchy"].encode("utf-8")
            fingerprint = sha256_bytes(png_bytes)

            png_key = storage.png_key(screen_id)
            hierarchy_key = storage.hierarchy_key(screen_id)
            storage.put_bytes(png_key, png_bytes)
            storage.put_bytes(hierarchy_key, hierarchy_bytes)

            cur.execute(
                INSERT_METADATA_SQL,
                (
                    screen_id,
                    row.get("app_package_name"),
                    row.get("category"),
                    png_key,
                    hierarchy_key,
                    run_id,
                    fingerprint,
                ),
            )
            screen_ids.append(screen_id)
            log.info(
                "run=%s stage=ingest screen=%s category=%r png_bytes=%d fingerprint=%s",
                run_id, screen_id, row.get("category"), len(png_bytes), fingerprint[:12],
            )
        conn.commit()

    log.info("run=%s stage=ingest complete screens=%d", run_id, len(screen_ids))
    return screen_ids
