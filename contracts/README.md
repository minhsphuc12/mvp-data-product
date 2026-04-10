# Data contracts

YAML files define expected physical shapes for both source and analytics landing layers:
- `contracts/schemas/`: staging landing tables in `analytics_db.staging` (used by dbt `sources.yml`).
- `contracts/source_schemas/`: operational source tables in `source_db_1` and `source_db_2`.

They complement dbt tests and support CI validation via `make validate-contracts`.

## Versioning

- **`contract_version`:** bump **minor** for backward-compatible column additions; **major** for renames, type changes, or dropped columns that break consumers.
- Align breaking changes with a migration checklist (staging → marts impact).

## Layout

- One file per table: `{schema}__{table}.yaml` (e.g. `staging__lending_loans.yaml`).
- Keep in sync with [infra/analytics_db_init/01_schemas_and_raw.sql](../infra/analytics_db_init/01_schemas_and_raw.sql) and [dbt_project/models/sources.yml](../dbt_project/models/sources.yml).
- Source contracts use `{system}__{table}.yaml` (e.g. `source_db_1__loans.yaml`) and stay in sync with:
  - [infra/source_db_1_init/01_schema.sql](../infra/source_db_1_init/01_schema.sql)
  - [infra/source_db_2_init/01_schema.sql](../infra/source_db_2_init/01_schema.sql)

## Optional: Soda / Great Expectations

For row-level checks in production, you can add Soda or GE on top of these contracts without replacing them. Example stub: [soda/example_checks.yml](soda/example_checks.yml).
