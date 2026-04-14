"""
Daily-style ELT DAG: operational Postgres -> analytics staging landing -> dbt layers.

Runs inside Docker Compose with profile `airflow`. Database hosts are the
Compose service names (source_db_1, source_db_2, analytics_db), not localhost.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

WORKSPACE = "/opt/airflow/workspace"
PYTHONPATH = WORKSPACE
DBT_DIR = f"{WORKSPACE}/dbt_project"

default_args = {
    "owner": "finance-demo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="finance_demo_daily",
    default_args=default_args,
    description="Ingest sources to staging landing, then dbt staging -> intermediate -> marts",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["finance_demo", "elt"],
) as dag:
    ingest_sources_to_staging = BashOperator(
        task_id="ingest_sources_to_staging",
        bash_command=f"""
        set -euo pipefail
        cd "{WORKSPACE}"
        export PYTHONPATH="{PYTHONPATH}"
        export LOAD_DATA_AS_OF="{{{{ ds }}}}"
        python -m data_gen.load_data
        """,
    )

    validate_scd2_seed = BashOperator(
        task_id="validate_scd2_seed",
        bash_command=f"""
        set -euo pipefail
        cd "{WORKSPACE}"
        export PYTHONPATH="{PYTHONPATH}"
        python scripts/validate_scd2_seed_history.py
        """,
    )

    # Single BashOperator for dbt avoids flaky `airflow dags test` runs where the
    # last task can remain "running" in metadata while the CLI marks the DagRun finished.
    dbt_run_and_test = BashOperator(
        task_id="dbt_run_and_test",
        bash_command=f"""
        set -euo pipefail
        export DBT_PROFILES_DIR="{DBT_DIR}"
        cd "{DBT_DIR}"
        dbt run --project-dir . --profiles-dir .
        dbt test --project-dir . --profiles-dir .
        """,
    )

    ingest_sources_to_staging >> validate_scd2_seed >> dbt_run_and_test
