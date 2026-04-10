#!/usr/bin/env python3
"""
Validate staging-landing YAML contracts against dbt sources.yml:
  - Every dbt-declared source table has a matching contract file.
  - Every column referenced in sources.yml exists in the contract.
  - No orphan contract files without a matching dbt source table.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = ROOT / "contracts" / "schemas"
SOURCES_PATH = ROOT / "dbt_project" / "models" / "sources.yml"


def _load_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}")
    return data


def _contract_key(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def load_contracts() -> dict[str, dict]:
    contracts: dict[str, dict] = {}
    if not CONTRACTS_DIR.is_dir():
        print(f"ERROR: contracts directory missing: {CONTRACTS_DIR}", file=sys.stderr)
        sys.exit(1)
    for path in sorted(CONTRACTS_DIR.glob("*.yaml")):
        doc = _load_yaml(path)
        schema = doc.get("schema")
        table = doc.get("table")
        if not schema or not table:
            print(f"ERROR: {path.name} must define schema and table", file=sys.stderr)
            sys.exit(1)
        key = _contract_key(schema, table)
        if key in contracts:
            print(f"ERROR: duplicate contract for {key}", file=sys.stderr)
            sys.exit(1)
        cols = doc.get("columns")
        if not isinstance(cols, dict) or not cols:
            print(f"ERROR: {path.name} must define non-empty columns", file=sys.stderr)
            sys.exit(1)
        contracts[key] = doc
    return contracts


def dbt_source_tables(sources_doc: dict) -> dict[str, set[str]]:
    """Map 'schema.table' -> set of column names declared in sources.yml."""
    out: dict[str, set[str]] = {}
    for src in sources_doc.get("sources", []):
        schema = src.get("schema") or src.get("name")
        if not schema:
            continue
        for tbl in src.get("tables", []):
            tname = tbl.get("name")
            if not tname:
                continue
            key = _contract_key(schema, tname)
            cols = out.setdefault(key, set())
            for col in tbl.get("columns", []):
                cname = col.get("name")
                if cname:
                    cols.add(cname)
    return out


def main() -> None:
    contracts = load_contracts()
    sources_doc = _load_yaml(SOURCES_PATH)
    dbt_tables = dbt_source_tables(sources_doc)

    errors: list[str] = []

    for key, dbt_cols in dbt_tables.items():
        if key not in contracts:
            errors.append(f"Missing contract file for dbt source {key} (expected {key.replace('.', '__')}.yaml under contracts/schemas/)")
            continue
        contract_cols = set(contracts[key]["columns"].keys())
        for c in sorted(dbt_cols):
            if c not in contract_cols:
                errors.append(f"Column '{c}' in dbt sources.yml for {key} not found in contract")

    for key in contracts:
        if key not in dbt_tables:
            errors.append(f"Orphan contract {key}: no matching table in dbt sources.yml")

    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        sys.exit(1)

    print(
        f"OK: {len(contracts)} staging landing table contracts aligned with dbt sources.yml",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
