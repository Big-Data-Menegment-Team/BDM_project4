"""Central configuration for the RICO pipeline.

Every value is environment-overridable; the defaults point at the sibling
docker-compose services (``postgres``, ``minio``, ``ollama``) and are injected
into the Airflow containers by ``docker-compose.yml``.
"""

import os

# --- Postgres (the `rico` data database) -----------------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "rico")
POSTGRES_USER = os.getenv("POSTGRES_USER", "rico")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "rico")
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
)

# --- MinIO / S3 -------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "rico-raw")

# --- Ollama -----------------------------------------------------------------
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")

# --- Dataset ----------------------------------------------------------------
HF_DATASET = os.getenv("HF_DATASET", "rootsautomation/RICO-Screen2Words")

# --- Model versions ---------------------------------------------------------
# Recorded in pipeline_runs so every row is traceable to the exact models used.
CLIP_ARCH = "ViT-B-32"
CLIP_PRETRAINED = "laion2b_s34b_b79k"
CLIP_MODEL_VERSION = f"open-clip-{CLIP_ARCH}-{CLIP_PRETRAINED.replace('_', '-')}"
SBERT_MODEL_VERSION = "sentence-transformers/all-MiniLM-L6-v2"
LLM_MODEL = OLLAMA_MODEL
PROMPT_VERSION = "v1"

# --- Pipeline defaults ------------------------------------------------------
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "5"))
