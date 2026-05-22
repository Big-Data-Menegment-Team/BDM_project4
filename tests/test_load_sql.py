"""Static checks on the load-stage SQL strings.

The load task owns idempotency, the cheapest regression guard is to assert
that every DELETE is scoped by the *natural key* (screen_id), not by run_id:
each Airflow re-trigger creates a fresh run_id, so a run_id-scoped DELETE
would leave the previous run's rows behind and the destination tables would
grow by N per re-trigger.
"""

from rico.tasks.load import (
    DELETE_EMBEDDINGS_FOR_SCREENS,
    DELETE_REVIEW_QUEUE_FOR_SCREENS,
    INSERT_EMBEDDING,
    INSERT_REVIEW_QUEUE,
    SELECT_RUN_SCREENS,
    UPDATE_METADATA_EXTRACTION,
)


def test_delete_embeddings_is_screen_scoped():
    assert "WHERE screen_id = ANY(%s)" in DELETE_EMBEDDINGS_FOR_SCREENS
    assert "WHERE run_id = %s" not in DELETE_EMBEDDINGS_FOR_SCREENS


def test_delete_review_queue_is_screen_scoped():
    assert "WHERE screen_id = ANY(%s)" in DELETE_REVIEW_QUEUE_FOR_SCREENS
    assert "WHERE run_id = %s" not in DELETE_REVIEW_QUEUE_FOR_SCREENS


def test_select_run_screens_is_run_scoped():
    assert "WHERE run_id = %s" in SELECT_RUN_SCREENS


def test_insert_embedding_carries_run_id_and_fingerprint():
    # Both columns are required by §3.2; a missing column here would write NULLs.
    assert "run_id" in INSERT_EMBEDDING
    assert "source_fingerprint" in INSERT_EMBEDDING


def test_insert_review_queue_carries_run_id_and_fingerprint():
    assert "run_id" in INSERT_REVIEW_QUEUE
    assert "source_fingerprint" in INSERT_REVIEW_QUEUE


def test_update_metadata_extraction_writes_jsonb_and_run_id():
    assert "extraction_payload = %s::jsonb" in UPDATE_METADATA_EXTRACTION
    assert "run_id" in UPDATE_METADATA_EXTRACTION
    assert "source_fingerprint" in UPDATE_METADATA_EXTRACTION
