"""MinIO / S3 helpers and the bucket key layout.

The key builders are the contract between producers and consumers of blobs:
``ingest`` writes the PNG and hierarchy JSON; ``parse`` writes the text rep;
``embed_*`` / ``extract`` read them back.
"""

import logging

import boto3

from rico import config

log = logging.getLogger(__name__)

_CLIENT = None


def s3_client():
    """Return a cached boto3 S3 client pointed at MinIO."""
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = boto3.client(
            "s3",
            endpoint_url=config.MINIO_ENDPOINT,
            aws_access_key_id=config.MINIO_ACCESS_KEY,
            aws_secret_access_key=config.MINIO_SECRET_KEY,
            region_name="us-east-1",
        )
    return _CLIENT


# --- Bucket key layout ------------------------------------------------------
def png_key(screen_id) -> str:
    """MinIO key for a screen's PNG screenshot."""
    return f"screens/{screen_id}.png"


def hierarchy_key(screen_id) -> str:
    """MinIO key for a screen's raw view-hierarchy JSON."""
    return f"screens/{screen_id}.json"


def text_key(screen_id) -> str:
    """MinIO key for a screen's parsed text representation."""
    return f"screens/{screen_id}.txt"


def clip_vector_key(screen_id) -> str:
    """MinIO key for a CLIP image vector staged by ``embed_image`` for ``load``.

    The staged object is a numpy ``.npz`` archive packing the vector and the
    PNG ``source_fingerprint`` together so ``load`` cannot read one without
    the other.
    """
    return f"screens/{screen_id}.clip.npz"


def sbert_vector_key(screen_id) -> str:
    """MinIO key for an SBERT text vector staged by ``embed_text`` for ``load``.

    Same ``.npz`` layout as :func:`clip_vector_key`; the packed fingerprint is
    the SHA-256 of the text representation the embedder consumed.
    """
    return f"screens/{screen_id}.sbert.npz"


def extraction_key(screen_id) -> str:
    """MinIO key for an LLM extraction staged by ``extract`` for ``load``."""
    return f"screens/{screen_id}.extract.json"


# --- Object I/O -------------------------------------------------------------
def put_bytes(key: str, data: bytes) -> None:
    """Write ``data`` to ``key`` in the configured bucket."""
    s3_client().put_object(Bucket=config.MINIO_BUCKET, Key=key, Body=data)


def get_bytes(key: str) -> bytes:
    """Read the object at ``key`` from the configured bucket."""
    obj = s3_client().get_object(Bucket=config.MINIO_BUCKET, Key=key)
    return obj["Body"].read()


def object_exists(key: str) -> bool:
    """Return whether ``key`` exists in the configured bucket."""
    from botocore.exceptions import ClientError

    try:
        s3_client().head_object(Bucket=config.MINIO_BUCKET, Key=key)
        return True
    except ClientError:
        return False
