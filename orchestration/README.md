# Orchestration (optional)

## Apache Airflow (recommended for scheduling)

Docker Compose profile **`airflow`** runs a local Airflow stack (metadata Postgres, scheduler, web UI) and a DAG that mimics a **daily** batch:

1. `ingest_sources_to_staging` — Python loader (`data_gen.load_data`) with `LOAD_DATA_AS_OF={{ ds }}` (operational DBs → `analytics_db.staging`)
2. `validate_scd2_seed` — `scripts/validate_scd2_seed_history.py`
3. `dbt_run_and_test` — `dbt run` then `dbt test` against `analytics_db`

From the repo root (after `make up` and `bash scripts/bootstrap.sh` so `dbt_project/profiles.yml` exists on the host mount):

```bash
make airflow-up
# UI: http://localhost:${AIRFLOW_WEBSERVER_PORT:-8080} — user `admin`, password `admin`
make airflow-dag-test # or: docker compose --env-file .env exec airflow_scheduler airflow dags test finance_demo_daily 2026-04-10
```

DAG code: [`airflow/dags/finance_demo_daily.py`](../airflow/dags/finance_demo_daily.py). Stop Airflow: `make airflow-down`.

## Prefect

Batch refresh as a **Prefect** flow for scheduling or local runs.

## Setup

```bash
pip install -r requirements-orchestration.txt
```

Ensure `.env`, Docker Postgres, and `dbt_project/profiles.yml` are configured like the main [README](../README.md).

## Run

From the repository root:

```bash
python orchestration/refresh_flow.py
```

This executes `make seed-data` then `make transform`. Replace `seed-data` with your production ELT entrypoint when moving off the demo loader.

## Deploy

Register the flow with a Prefect server or use Prefect Cloud; set infrastructure (Docker/K8s worker) to run commands in an environment that has `make`, Python, dbt, and DB connectivity.
