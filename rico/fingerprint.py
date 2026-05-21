"""SHA-256 helpers for record traceability (the `source_fingerprint` column).

Every destination row stores a `source_fingerprint`: a hash of the exact input
that produced it. This lets a run answer "did the model see exactly this byte
sequence?" without keeping the bytes themselves in Postgres.
"""

import hashlib


def sha256_bytes(payload: bytes) -> str:
    """Return the lowercase hex SHA-256 digest of ``payload``."""
    return hashlib.sha256(payload).hexdigest()


def sha256_text(text: str) -> str:
    """Return the SHA-256 digest of ``text`` encoded as UTF-8."""
    return sha256_bytes(text.encode("utf-8"))
