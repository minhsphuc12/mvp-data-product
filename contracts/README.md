# Data contracts (raw layer)

YAML files under `schemas/` describe **expected physical shape** of mirrored raw tables: names, logical types, nullability, and enums where applicable. They complement dbt `sources.yml` tests and support CI validation via `make validate-contracts`.

## Versioning

- **`contract_version`:** bump **minor** for backward-compatible column additions; **major** for renames, type changes, or dropped columns that break consumers.
- Align breaking changes with a migration checklist (staging → marts impact).

## Layout

- One file per table: `{schema}__{table}.yaml` (e.g. `raw_lending__loans.yaml`).
- Keep in sync with [infra/analytics_db_init/01_schemas_and_raw.sql](../infra/analytics_db_init/01_schemas_and_raw.sql) and [dbt_project/models/sources.yml](../dbt_project/models/sources.yml).

## Optional: Soda / Great Expectations

For row-level checks in production, you can add Soda or GE on top of these contracts without replacing them. Example stub: [soda/example_checks.yml](soda/example_checks.yml).
