"""parse stage — translate each screen's view hierarchy into a text representation.

Translated from Section 2 of the lab notebook. The two pure functions
(``parse_hierarchy``, ``text_representation``) carry the logic and are unit
tested; ``run_parse`` is the Airflow-facing wrapper that does the MinIO I/O.

Per the team's design decision, parse persists each text representation as
``screens/{id}.txt`` in MinIO so the parallel ``embed_text`` and ``extract``
tasks can read it independently.
"""

import json
import logging

log = logging.getLogger(__name__)


def parse_hierarchy(
    raw_json: str,
) -> list[tuple[str, str, tuple[int, int, int, int]]]:
    """Iterative DFS — returns (element_type, text, bounds) for nodes with text or class."""
    tree = json.loads(raw_json)
    # RICO wraps the real tree in {"activity": {"root": ...}}; unwrap if present.
    root = tree.get("activity", {}).get("root", tree) if isinstance(tree, dict) else None

    elements: list[tuple[str, str, tuple[int, int, int, int]]] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        text = (node.get("text") or "").strip()
        cls = (node.get("class") or "").strip()
        if text or cls:
            element_type = cls.rsplit(".", 1)[-1] if cls else ""
            raw_bounds = node.get("bounds") or [0, 0, 0, 0]
            bounds = tuple(int(b) for b in raw_bounds) if len(raw_bounds) == 4 else (0, 0, 0, 0)
            elements.append((element_type, text, bounds))
        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))
    return elements


def text_representation(
    elements: list[tuple[str, str, tuple[int, int, int, int]]],
) -> str:
    """Concatenate texts in reading order: sort by (y_top, x_left), join with spaces."""
    with_text = [e for e in elements if e[1]]
    in_order = sorted(with_text, key=lambda e: (e[2][1], e[2][0]))
    return " ".join(text for _type, text, _bounds in in_order)


def run_parse(run_id: str) -> dict:
    """Parse every screen ingested by this run; write text reps back to MinIO.

    Reads the run's rows from ``screens_metadata`` (so it depends only on
    ``run_id``), fetches each view-hierarchy JSON from MinIO, and stores the
    derived text representation as ``screens/{id}.txt``.
    """
    from rico import db, storage

    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT screen_id, hierarchy_json_path FROM screens_metadata "
            "WHERE run_id = %s ORDER BY screen_id",
            (run_id,),
        )
        rows = cur.fetchall()

    parsed = 0
    for screen_id, hierarchy_key in rows:
        raw = storage.get_bytes(hierarchy_key).decode("utf-8")
        elements = parse_hierarchy(raw)
        text = text_representation(elements)
        storage.put_bytes(storage.text_key(screen_id), text.encode("utf-8"))
        parsed += 1
        log.info(
            "run=%s stage=parse screen=%s elements=%d text_chars=%d",
            run_id, screen_id, len(elements), len(text),
        )

    log.info("run=%s stage=parse complete screens=%d", run_id, parsed)
    return {"run_id": run_id, "screens_parsed": parsed}
