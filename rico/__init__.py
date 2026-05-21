"""RICO production pipeline — shared package for the Airflow DAG.

Business logic lives here; the DAG file (``dags/rico_pipeline.py``) only wires
tasks together. See ``TASK_SPLIT.md`` for how the work is divided across the team.
"""

__version__ = "0.1.0"
