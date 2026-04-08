# Realtime sources and CDC (extensions)

The default demo uses **batch** Python loads into `raw_*` schemas. This page outlines how to evolve toward **lower latency** and **change capture** without locking you into one vendor.

## What is already enabled in Compose

- **`wal_level=logical`** on `source_db_1` and `source_db_2` so PostgreSQL can emit logical replication/decoding when you add a replication slot (required for native CDC tools).
- **Incremental staging example:** `stg_lending_loans` is an **incremental** model merged on `loan_id`, filtered by `loaded_at`. After a CDC or micro-batch load updates `raw_lending.loans`, a dbt run merges new/changed rows. Use `--full-refresh` for the first build or after logic changes.

## Pattern: CDC → raw → dbt

1. **Capture:** Debezium, native logical replication, or an ELT tool (Airbyte, Fivetran-class) reads the operational DB.
2. **Land:** Append or upsert into `raw_lending` / `raw_insurance` (keep `loaded_at` or a CDC `__deleted` column if you model deletes).
3. **Transform:** Schedule `dbt run` (every N minutes or on completion of load). Incremental models use `loaded_at` or high-water marks.

## Pattern: Event stream → warehouse

1. Publish domain events (Kafka / Redpanda).
2. Consumer writes to Postgres `raw_*` or to a columnar warehouse.
3. Separate **event** reporting from **monthly mart** grain: facts can be intraday; `mart_branch_monthly_performance` can remain batch rollup.

## Orchestration

- **Prefect (optional in this repo):** [orchestration/refresh_flow.py](../orchestration/refresh_flow.py) runs `make seed-data` and `make transform`. Install with `pip install -r requirements-orchestration.txt`.
- **Production:** Airflow, Dagster, or cloud schedulers trigger the same steps against a real warehouse.

## Operational checks

- Monitor **replication lag** and **max(loaded_at)** per raw table.
- Alert when dbt tests fail or row counts drop unexpectedly (see [contracts/README.md](../contracts/README.md) and optional Soda stub under `contracts/soda/`).
