"""extract stage - LLM structured extraction staged in MinIO for ``load``.

The prompt is loaded from a *versioned* file (``rico/prompts/extract_v{PROMPT_VERSION}.txt``) 
instead of an inline string. ``rico.config.PROMPT_VERSION`` is the contract.
The notebook raises ``JSONDecodeError`` on bad LLM output and dies.
This task catches the failure per screen and stages an error payload, the ``load`` task routes the affected screens to ``screens_review_queue``.

Output: one ``screens/{id}.extract.json`` per screen in MinIO. 
Shape:
  {"ok": true, "payload": {...}, "fingerprint": "<sha256 of text rep>"}
  {"ok": false, "raw": "...", "error": "...", "fingerprint": "<sha256>"}
"""

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

SELECT_RUN_SCREENS = (
    "SELECT screen_id FROM screens_metadata "
    "WHERE run_id = %s ORDER BY screen_id"
)


def load_prompt_template() -> str:
    """Read ``rico/prompts/extract_{PROMPT_VERSION}.txt`` from the package dir."""
    from rico import config

    prompts_dir = Path(__file__).resolve().parent.parent / "prompts"
    path = prompts_dir / f"extract_{config.PROMPT_VERSION}.txt"
    return path.read_text(encoding="utf-8")


def call_ollama(prompt: str, timeout: int = 120) -> str:
    """POST the prompt to Ollama's ``/api/generate``, return the raw response text."""
    import requests

    from rico import config

    resp = requests.post(
        f"{config.OLLAMA_URL}/api/generate",
        json={"model": config.OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()["response"]


def run_extract(run_id: str) -> dict:
    """Run LLM extraction on every parsed text added for this run.

    Stages one ``screens/{id}.extract.json`` artifact per screen in MinIO.
    Returns a small summary dict for logging, ``load`` is the consumer.
    """
    from rico import config, db, storage
    from rico.fingerprint import sha256_text

    template = load_prompt_template()
    log.info(
        "run=%s stage=extract starting prompt_version=%s bytes=%d model=%s",
        run_id, config.PROMPT_VERSION, len(template), config.OLLAMA_MODEL,
    )

    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(SELECT_RUN_SCREENS, (run_id,))
        rows = cur.fetchall()

    if not rows:
        log.warning("run=%s stage=extract no screens to extract", run_id)
        return {"run_id": run_id, "extracted": 0, "ok": 0, "failed": 0}

    ok_count = 0
    failed_count = 0
    for (screen_id,) in rows:
        text = storage.get_bytes(storage.text_key(int(screen_id))).decode("utf-8")
        fingerprint = sha256_text(text)
        prompt = template.replace("{hierarchy_text}", text)

        try:
            raw = call_ollama(prompt)
            payload = json.loads(raw)
            artifact: dict = {
                "ok": True,
                "payload": payload,
                "fingerprint": fingerprint,
            }
            ok_count += 1
            log.info(
                "run=%s stage=extract screen=%s ok=true payload_keys=%s fingerprint=%s",
                run_id, screen_id, sorted(payload.keys()) if isinstance(payload, dict) else None,
                fingerprint[:12],
            )
        except Exception as exc:  # bad JSON, HTTP errors, timeouts all route to review
            raw_text = locals().get("raw")
            artifact = {
                "ok": False,
                "raw": raw_text if isinstance(raw_text, str) else None,
                "error": f"{type(exc).__name__}: {exc}",
                "fingerprint": fingerprint,
            }
            failed_count += 1
            log.warning(
                "run=%s stage=extract screen=%s ok=false error=%s",
                run_id, screen_id, artifact["error"],
            )

        storage.put_bytes(
            storage.extraction_key(int(screen_id)),
            json.dumps(artifact).encode("utf-8"),
        )

    log.info(
        "run=%s stage=extract complete screens=%d ok=%d failed=%d",
        run_id, len(rows), ok_count, failed_count,
    )
    return {
        "run_id": run_id,
        "extracted": len(rows),
        "ok": ok_count,
        "failed": failed_count,
    }
