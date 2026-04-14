SHELL := /bin/bash
ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
export PYTHONPATH := $(ROOT)
PY := $(if $(wildcard $(ROOT).venv/bin/python),$(ROOT).venv/bin/python,python3)
DBT := $(if $(wildcard $(ROOT).venv/bin/dbt),$(ROOT).venv/bin/dbt,dbt)

.PHONY: up down bi-up bi-down airflow-up airflow-down airflow-dag-test seed-data transform test docs lineage demo bootstrap check-dbt-python validate-contracts flow-refresh semantic-build semantic-validate analytics-bootstrap-bi validate-scd2-seed

# dbt-core fails on Python 3.14+; ensure .venv uses 3.11–3.13 (recreate via scripts/bootstrap.sh).
check-dbt-python:
	@$(PY) -c 'import sys; v=sys.version_info[:2]; sys.exit(0 if (3,11)<=v<=(3,13) else 1)' || { ver=$$($(PY) -c 'import sys; print("%d.%d" % sys.version_info[:2])'); echo "Python 3.11-3.13 required for dbt (found $$ver). Run: rm -rf .venv && bash scripts/bootstrap.sh" >&2; exit 1; }

up:
	docker compose --env-file .env up -d

down:
	docker compose --env-file .env down

bi-up:
	docker compose --env-file .env --profile bi up -d metabase

bi-down:
	docker compose --env-file .env --profile bi stop metabase

# Apache Airflow (profile airflow).
airflow-up:
	docker compose --env-file .env --profile airflow up -d --build

airflow-down:
	docker compose --env-file .env --profile airflow down

# One inline DAG run (loads data for logical date, validates SCD2 seed, dbt run + test). Requires: make up && make airflow-up
airflow-dag-test:
	docker compose --env-file .env --profile airflow exec airflow_scheduler airflow dags test finance_demo_daily 2026-04-10

seed-data:
	set -a && source .env && set +a && $(PY) -m data_gen.load_data && $(PY) "$(ROOT)scripts/validate_scd2_seed_history.py"

validate-scd2-seed:
	set -a && source .env && set +a && $(PY) "$(ROOT)scripts/validate_scd2_seed_history.py"

transform: check-dbt-python
	set -a && source .env && set +a && \
	export DBT_PROFILES_DIR="$(ROOT)dbt_project" && \
	cd dbt_project && $(DBT) run --project-dir . --profiles-dir .

test: check-dbt-python
	set -a && source .env && set +a && \
	export DBT_PROFILES_DIR="$(ROOT)dbt_project" && \
	cd dbt_project && $(DBT) test --project-dir . --profiles-dir .

validate-contracts:
	$(PY) "$(ROOT)scripts/validate_data_contracts.py"
	$(PY) "$(ROOT)scripts/validate_source_contracts.py"

flow-refresh:
	$(PY) "$(ROOT)orchestration/refresh_flow.py"

semantic-build:
	set -a && source .env && set +a && \
	$(PY) "$(ROOT)scripts/build_semantic_artifacts.py"

semantic-validate:
	set -a && source .env && set +a && \
	$(PY) "$(ROOT)scripts/validate_semantic_artifacts.py"

# Grants bi_business_readonly on marts + semantic, then creates login ANALYTICS_DB_USER_BI. Requires: make up, and usually semantic-build first.
analytics-bootstrap-bi:
	set -a && source "$(ROOT).env" && set +a && \
	test -n "$$ANALYTICS_DB_USER_BI" && test -n "$$ANALYTICS_DB_PASSWORD_BI" || { echo "Set ANALYTICS_DB_USER_BI and ANALYTICS_DB_PASSWORD_BI in .env" >&2; exit 1; } && \
	cd "$(ROOT)" && \
	docker compose --env-file .env exec -T \
	  -e "PGPASSWORD=$$ANALYTICS_DB_PASSWORD" \
	  analytics_db \
	  psql -U "$$ANALYTICS_DB_USER" -d "$$ANALYTICS_DB_DATABASE" -v ON_ERROR_STOP=1 \
	  < bi/sql/metabase_business_permissions.sql && \
	docker compose --env-file .env exec -T \
	  -e "PGPASSWORD=$$ANALYTICS_DB_PASSWORD" \
	  analytics_db \
	  psql -U "$$ANALYTICS_DB_USER" -d "$$ANALYTICS_DB_DATABASE" -v ON_ERROR_STOP=1 \
	  -v "metabase_bi_user=$$ANALYTICS_DB_USER_BI" \
	  -v "metabase_bi_password=$$ANALYTICS_DB_PASSWORD_BI" \
	  < bi/sql/bootstrap_metabase_business_user.sql

docs: check-dbt-python
	set -a && source .env && set +a && \
	export DBT_PROFILES_DIR="$(ROOT)dbt_project" && \
	cd dbt_project && $(DBT) docs generate --project-dir . --profiles-dir .

lineage:
	$(PY) "$(ROOT)lineage/render_lineage.py" \
		--manifest "$(ROOT)dbt_project/target/manifest.json" \
		--output "$(ROOT)lineage/lineage.mmd"

# Demo target that includes all components: main stack, BI (Metabase), Airflow, and all validation/test/docs/lineage steps.
#bi-up
#airflow-up
demo: up 
	@echo "Waiting for Postgres and other services healthchecks..."
	@sleep 2
	$(MAKE) seed-data transform validate-contracts test docs lineage 
	$(MAKE) semantic-build semantic-validate analytics-bootstrap-bi

#$(MAKE) airflow-dag-test

bootstrap:
	bash "$(ROOT)scripts/bootstrap.sh"
