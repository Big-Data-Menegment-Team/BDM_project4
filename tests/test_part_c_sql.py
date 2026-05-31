"""Regression guards for Part C SQL contracts and summary formatting."""

from rico.observability import UPSERT_METRIC_SQL, format_summary_line
from rico.tasks.audit import (
    INSERT_AUDIT_RESULT,
    SELECT_EMBEDDING_DUPLICATES,
    SELECT_METADATA_DUPLICATES_FOR_RUN,
)
from rico.tasks.eval import DELETE_EVAL_FOR_RUN, INSERT_EVAL, NEAREST_TEXT_SQL


def test_audit_duplicate_query_checks_full_embedding_key():
    assert "GROUP BY screen_id, model_name, model_version, embedding_kind" in SELECT_EMBEDDING_DUPLICATES
    assert "HAVING count(*) > 1" in SELECT_EMBEDDING_DUPLICATES


def test_audit_metadata_query_is_run_scoped():
    assert "WHERE run_id = %s" in SELECT_METADATA_DUPLICATES_FOR_RUN
    assert "HAVING count(*) > 1" in SELECT_METADATA_DUPLICATES_FOR_RUN


def test_audit_result_persists_json_details():
    assert "INSERT INTO audit_results" in INSERT_AUDIT_RESULT
    assert "details" in INSERT_AUDIT_RESULT
    assert "%s::jsonb" in INSERT_AUDIT_RESULT


def test_eval_query_is_run_scoped_and_top_k():
    assert "WHERE run_id = %s AND embedding_kind = 'text'" in NEAREST_TEXT_SQL
    assert "LIMIT %s" in NEAREST_TEXT_SQL


def test_eval_insert_and_idempotent_delete_are_present():
    assert "DELETE FROM screens_eval WHERE run_id = %s" == DELETE_EVAL_FOR_RUN
    assert "INSERT INTO screens_eval" in INSERT_EVAL
    assert "recall_at_5" in INSERT_EVAL


def test_metrics_upsert_is_unique_on_run_and_name():
    assert "ON CONFLICT (run_id, metric_name) DO UPDATE" in UPSERT_METRIC_SQL
    assert "metric_detail" in UPSERT_METRIC_SQL


def test_summary_line_includes_required_quality_fields():
    line = format_summary_line(
        {
            "final_status": "succeeded",
            "total_run_duration_sec": 12.34,
            "metadata_rows": 5,
            "extraction_non_null_pct": 80.0,
            "confidence_ge_05_pct": 60.0,
            "review_queue_pct": 20.0,
            "embeddings_rows": 10,
            "zero_norm_pct": 0.0,
            "recall_at_5": 1.0,
        }
    )
    assert "status=succeeded" in line
    assert "metadata_rows=5" in line
    assert "zero_norm_pct=0.0" in line
    assert "recall_at_5=1.000" in line
