"""eval stage - retrieval quality (recall@5) for the current run (Part C)."""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

SELECT_RUN_SCREEN_IDS = """
SELECT screen_id
FROM screens_metadata
WHERE run_id = %s
ORDER BY screen_id
"""

NEAREST_TEXT_SQL = """
SELECT screen_id
FROM screens_embeddings
WHERE run_id = %s AND embedding_kind = 'text'
ORDER BY vector <-> %s::vector
LIMIT %s
"""

DELETE_EVAL_FOR_RUN = "DELETE FROM screens_eval WHERE run_id = %s"

INSERT_EVAL = """
INSERT INTO screens_eval (embedding_model_version, n_queries, recall_at_5, run_id)
VALUES (%s, %s, %s, %s)
"""


def run_eval(run_id: str) -> dict:
    """Compute recall@5 using same-run SBERT text retrieval and persist it."""
    from sentence_transformers import SentenceTransformer

    from rico import config, db, storage

    with db.connection(register_pgvector=True) as conn, conn.cursor() as cur:
        cur.execute(SELECT_RUN_SCREEN_IDS, (run_id,))
        screen_ids = [int(screen_id) for (screen_id,) in cur.fetchall()]

    if not screen_ids:
        log.warning("run=%s stage=eval no screens found for run", run_id)
        return {"run_id": run_id, "n_queries": 0, "recall_at_5": 0.0, "k": 5}

    queries: list[tuple[int, str]] = []
    for screen_id in screen_ids:
        key = storage.text_key(screen_id)
        try:
            text = storage.get_bytes(key).decode("utf-8")
        except Exception as exc:
            log.warning(
                "run=%s stage=eval screen=%s missing_text key=%s error=%s",
                run_id,
                screen_id,
                key,
                f"{type(exc).__name__}: {exc}",
            )
            continue
        if text.strip():
            queries.append((screen_id, text))

    if not queries:
        log.warning("run=%s stage=eval no valid text queries", run_id)
        return {"run_id": run_id, "n_queries": 0, "recall_at_5": 0.0, "k": 5}

    sbert = SentenceTransformer(config.SBERT_MODEL_VERSION)
    encoded = sbert.encode(
        [query_text for (_expected_id, query_text) in queries],
        normalize_embeddings=True,
    ).astype("float32")

    hits = 0
    k = 5
    detail: list[tuple[int, list[int]]] = []
    with db.connection(register_pgvector=True) as conn, conn.cursor() as cur:
        for (expected_id, _), qvec in zip(queries, encoded):
            cur.execute(NEAREST_TEXT_SQL, (run_id, qvec, k))
            top_ids = [int(screen_id) for (screen_id,) in cur.fetchall()]
            detail.append((expected_id, top_ids))
            if expected_id in top_ids:
                hits += 1

        recall_at_5 = float(hits) / float(len(queries))
        cur.execute(DELETE_EVAL_FOR_RUN, (run_id,))
        cur.execute(
            INSERT_EVAL,
            (config.SBERT_MODEL_VERSION, len(queries), recall_at_5, run_id),
        )
        conn.commit()

    log.info(
        "run=%s stage=eval complete n_queries=%d hits=%d recall_at_5=%.4f",
        run_id,
        len(queries),
        hits,
        recall_at_5,
    )
    for expected_id, top_ids in detail:
        log.info(
            "run=%s stage=eval expected_screen=%s top5=%s",
            run_id,
            expected_id,
            top_ids,
        )

    return {
        "run_id": run_id,
        "n_queries": len(queries),
        "recall_at_5": recall_at_5,
        "k": k,
    }
