# Orchestration (optional)

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
