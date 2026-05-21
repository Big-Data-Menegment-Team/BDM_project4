"""Pipeline stage implementations — one module per Airflow task.

Each module exposes a ``run_<stage>(...)`` function that the DAG calls. Heavy
imports (torch, boto3, datasets) are deferred into those functions so the DAG
file stays fast to parse.
"""
