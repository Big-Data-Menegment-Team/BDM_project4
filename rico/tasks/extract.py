"""extract stage — PART B (stub, not yet implemented).

Owner: Part B. Reference: Section 5 of the lab notebook. See TASK_SPLIT.md.
"""

import logging

log = logging.getLogger(__name__)


def run_extract(run_id: str):
    """Run the LLM structured-extraction over each screen's text representation.

    Contract for Part B:
      * Find this run's screens with ``screens_metadata WHERE run_id = %s``.
      * Read each text rep from MinIO via ``rico.storage.text_key(screen_id)``.
      * Call Ollama at ``rico.config.OLLAMA_URL`` with ``rico.config.OLLAMA_MODEL``.
      * Use a *versioned* prompt — create ``rico/prompts/extract_v1.txt`` and keep
        ``rico.config.PROMPT_VERSION`` in sync.
      * On valid JSON: hand the extraction to ``load`` (writes screens_metadata).
      * On invalid JSON: route the screen to ``screens_review_queue`` instead of
        crashing the task.
      * ``source_fingerprint = rico.fingerprint.sha256_text(<text rep>)``.
    """
    raise NotImplementedError(
        "extract is owned by Part B — see TASK_SPLIT.md (Part B)"
    )
