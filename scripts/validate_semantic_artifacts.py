#!/usr/bin/env python3
"""
Validate semantic curated artifacts in analytics_db.

Checks:
  - All contract artifacts exist as views under semantic schema.
  - Core aggregate totals in semantic views reconcile with source mart totals.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
import yaml
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "bi" / "semantic" / "contract.yml"
SEMANTIC_SCHEMA = "semantic"


def _load_contract(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}")
    return data


def _get_dsn() -> str:
    url = os.getenv("ANALYTICS_DATABASE_URL")
    if url:
        return url

    host = os.getenv("ANALYTICS_DB_HOST", "localhost")
    port = os.getenv("ANALYTICS_DB_PORT", "5435")
    user = os.getenv("ANALYTICS_DB_USER", "demo")
    password = os.getenv("ANALYTICS_DB_PASSWORD", "demo")
    database = os.getenv("ANALYTICS_DB_DATABASE", "analytics_db")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def main() -> None:
    contract = _load_contract(CONTRACT_PATH)
    artifacts = contract.get("artifacts", [])
    source_schema = contract["source"]["schema"]
    source_table = contract["source"]["table"]

    checks_ok = True
    with psycopg2.connect(_get_dsn()) as conn, conn.cursor() as cur:
        for artifact in artifacts:
            view_name = artifact["name"]
            cur.execute(
                """
                select 1
                from information_schema.views
                where table_schema = %s
                  and table_name = %s
                """,
                (SEMANTIC_SCHEMA, view_name),
            )
            if cur.fetchone() is None:
                print(
                    f"ERROR: Missing semantic view {SEMANTIC_SCHEMA}.{view_name}",
                    file=sys.stderr,
                )
                checks_ok = False

        # Reconciliation checks for key totals.
        cur.execute(
            sql.SQL(
                """
                with src as (
                    select
                        coalesce(sum(loan_disbursement_amount), 0) as total_loan_disbursement_amount,
                        coalesce(sum(repayment_amount), 0) as total_repayment_amount,
                        coalesce(sum(claim_amount), 0) as total_claim_amount
                    from {}.{}
                ),
                sem as (
                    select
                        coalesce(sum(total_loan_disbursement_amount), 0) as total_loan_disbursement_amount,
                        coalesce(sum(total_repayment_amount), 0) as total_repayment_amount,
                        coalesce(sum(total_claim_amount), 0) as total_claim_amount
                    from {}.executive_monthly_trend
                )
                select
                    (src.total_loan_disbursement_amount = sem.total_loan_disbursement_amount) as disbursement_ok,
                    (src.total_repayment_amount = sem.total_repayment_amount) as repayment_ok,
                    (src.total_claim_amount = sem.total_claim_amount) as claim_ok
                from src
                cross join sem
                """
            ).format(
                sql.Identifier(source_schema),
                sql.Identifier(source_table),
                sql.Identifier(SEMANTIC_SCHEMA),
            )
        )
        disbursement_ok, repayment_ok, claim_ok = cur.fetchone()
        if not all([disbursement_ok, repayment_ok, claim_ok]):
            print(
                "ERROR: Reconciliation failed between source mart and semantic executive_monthly_trend",
                file=sys.stderr,
            )
            checks_ok = False

    if not checks_ok:
        sys.exit(1)
    print("OK: semantic artifacts exist and reconcile with source mart totals")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - CLI script
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
