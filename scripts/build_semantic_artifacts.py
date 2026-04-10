#!/usr/bin/env python3
"""
Build curated semantic artifacts in analytics_db for BI tools.

Artifacts are defined in bi/semantic/contract.yml and materialized under
the semantic schema as views.
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
    # Prefer explicit URL; fallback to assembled DSN from ANALYTICS_DB_* values.
    url = os.getenv("ANALYTICS_DATABASE_URL")
    if url:
        return url

    host = os.getenv("ANALYTICS_DB_HOST", "localhost")
    port = os.getenv("ANALYTICS_DB_PORT", "5435")
    user = os.getenv("ANALYTICS_DB_USER", "demo")
    password = os.getenv("ANALYTICS_DB_PASSWORD", "demo")
    database = os.getenv("ANALYTICS_DB_DATABASE", "analytics_db")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _validate_contract(contract: dict) -> None:
    if "source" not in contract or "artifacts" not in contract:
        raise ValueError("contract.yml must contain source and artifacts")
    source = contract["source"]
    if not source.get("schema") or not source.get("table"):
        raise ValueError("contract.yml source must define schema and table")
    artifacts = contract["artifacts"]
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("contract.yml artifacts must be a non-empty list")
    for item in artifacts:
        if not item.get("name") or not item.get("sql"):
            raise ValueError("each artifact must define name and sql")
        if item.get("type", "view") != "view":
            raise ValueError("only type=view is supported")


def main() -> None:
    contract = _load_contract(CONTRACT_PATH)
    _validate_contract(contract)

    source_schema = contract["source"]["schema"]
    source_table = contract["source"]["table"]
    source_relation = f"{source_schema}.{source_table}"
    dsn = _get_dsn()

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("create schema if not exists {}").format(
                    sql.Identifier(SEMANTIC_SCHEMA)
                )
            )

            for artifact in contract["artifacts"]:
                name = artifact["name"]
                query = artifact["sql"].strip().format(source_relation=source_relation)
                cur.execute(
                    sql.SQL("create or replace view {}.{} as {}").format(
                        sql.Identifier(SEMANTIC_SCHEMA),
                        sql.Identifier(name),
                        sql.SQL(query),
                    )
                )
                print(f"Built view: {SEMANTIC_SCHEMA}.{name}")

    print("Semantic artifacts build completed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - CLI script
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
