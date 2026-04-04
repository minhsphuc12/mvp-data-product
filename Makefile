SHELL := /bin/bash
ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
export PYTHONPATH := $(ROOT)
PY := $(if $(wildcard $(ROOT).venv/bin/python),$(ROOT).venv/bin/python,python3)
DBT := $(if $(wildcard $(ROOT).venv/bin/dbt),$(ROOT).venv/bin/dbt,dbt)

.PHONY: up down seed-data transform test docs lineage demo bootstrap

up:
	docker compose --env-file .env up -d

down:
	docker compose --env-file .env down

seed-data:
	set -a && source .env && set +a && $(PY) -m data_gen.load_data

transform:
	set -a && source .env && set +a && \
	export DBT_PROFILES_DIR="$(ROOT)dbt_project" && \
	cd dbt_project && $(DBT) run --project-dir . --profiles-dir .

test:
	set -a && source .env && set +a && \
	export DBT_PROFILES_DIR="$(ROOT)dbt_project" && \
	cd dbt_project && $(DBT) test --project-dir . --profiles-dir .

docs:
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
	$(MAKE) seed-data transform test docs lineage

bootstrap:
	bash "$(ROOT)scripts/bootstrap.sh"
