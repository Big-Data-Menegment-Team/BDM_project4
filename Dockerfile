# Custom Airflow image: Airflow 2.10 + the RICO ML stack.
#
# The embed/extract tasks run inside the Airflow worker, so torch, open-clip
# and sentence-transformers must live in this image alongside Airflow.
FROM apache/airflow:2.10.5-python3.11

# Install CPU-only torch first. The default PyPI index pulls the multi-GB CUDA
# build, which is dead weight on this stack — pin it to the CPU wheel index.
RUN pip install --no-cache-dir \
    --index-url https://download.pytorch.org/whl/cpu \
    "torch>=2.4"

# Install the `rico` package and its remaining dependencies. torch is already
# satisfied by the CPU build above, so pip will not re-pull it.
COPY --chown=airflow:root pyproject.toml /opt/rico-src/pyproject.toml
COPY --chown=airflow:root rico/ /opt/rico-src/rico/
RUN pip install --no-cache-dir /opt/rico-src

# Pre-create the HuggingFace cache directory, owned by the airflow user. When
# the `hf-cache` named volume is first mounted here, Docker initialises the
# volume from this directory — so it inherits airflow ownership instead of
# root, and model/dataset downloads can write to it.
RUN mkdir -p /opt/airflow/.cache/huggingface
