SHELL := /bin/bash
ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
export PYTHONPATH := $(ROOT)
PY := $(if $(wildcard $(ROOT).venv/bin/python),$(ROOT).venv/bin/python,python3)
DBT := $(if $(wildcard $(ROOT).venv/bin/dbt),$(ROOT).venv/bin/dbt,dbt)

.PHONY: up down bi-up bi-down seed-data transform test docs lineage demo bootstrap check-dbt-python validate-contracts flow-refresh semantic-build semantic-validate validate-scd2-seed

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

docs: check-dbt-python
	set -a && source .env && set +a && \
	export DBT_PROFILES_DIR="$(ROOT)dbt_project" && \
	cd dbt_project && $(DBT) docs generate --project-dir . --profiles-dir .

lineage:
	$(PY) "$(ROOT)lineage/render_lineage.py" \
		--manifest "$(ROOT)dbt_project/target/manifest.json" \
		--output "$(ROOT)lineage/lineage.mmd"

demo: up
	@echo "Waiting for Postgres healthchecks..."
	@sleep 8
	$(MAKE) validate-contracts seed-data transform semantic-build semantic-validate test docs lineage

bootstrap:
	bash "$(ROOT)scripts/bootstrap.sh"
