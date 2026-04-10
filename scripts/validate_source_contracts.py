#!/usr/bin/env python3
"""
Validate source-level YAML contracts against source DB init SQL files.

Checks:
- every expected source table has a contract file
- contract table/columns match DDL table/columns
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = ROOT / "contracts" / "source_schemas"
DDL_PATHS = {
    "source_db_1": ROOT / "infra" / "source_db_1_init" / "01_schema.sql",
    "source_db_2": ROOT / "infra" / "source_db_2_init" / "01_schema.sql",
}

CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(?P<table>[a-zA-Z_][a-zA-Z0-9_]*)\s*\((?P<body>.*?)\);",
    re.IGNORECASE | re.DOTALL,
)


def _load_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}")
    return data


def _parse_columns(body: str) -> set[str]:
    cols: set[str] = set()
    for raw_line in body.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue
        upper = line.upper()
        if upper.startswith(("PRIMARY KEY", "FOREIGN KEY", "CONSTRAINT", "UNIQUE", "CHECK")):
            continue
        m = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s+", line)
        if m:
            cols.add(m.group(1))
    return cols


def parse_ddl_tables(path: Path) -> dict[str, set[str]]:
    sql = path.read_text(encoding="utf-8")
    out: dict[str, set[str]] = {}
    for m in CREATE_TABLE_RE.finditer(sql):
        out[m.group("table")] = _parse_columns(m.group("body"))
    return out


def load_contracts() -> dict[str, dict[str, set[str]]]:
    result: dict[str, dict[str, set[str]]] = {}
    for path in sorted(CONTRACTS_DIR.glob("*.yaml")):
        doc = _load_yaml(path)
        system = doc.get("system")
        table = doc.get("table")
        columns = doc.get("columns")
        if not system or not table or not isinstance(columns, dict) or not columns:
            raise ValueError(f"Invalid contract file: {path.name}")
        result.setdefault(system, {})[table] = set(columns.keys())
    return result


def main() -> None:
    if not CONTRACTS_DIR.is_dir():
        print(f"ERROR: contracts directory missing: {CONTRACTS_DIR}", file=sys.stderr)
        sys.exit(1)

    contracts = load_contracts()
    errors: list[str] = []

    for system, ddl_path in DDL_PATHS.items():
        ddl_tables = parse_ddl_tables(ddl_path)
        contract_tables = contracts.get(system, {})

        for table, ddl_cols in ddl_tables.items():
            if table not in contract_tables:
                errors.append(f"Missing source contract for {system}.{table}")
                continue
            missing_cols = ddl_cols - contract_tables[table]
            extra_cols = contract_tables[table] - ddl_cols
            if missing_cols:
                errors.append(f"{system}.{table} missing columns in contract: {sorted(missing_cols)}")
            if extra_cols:
                errors.append(f"{system}.{table} has extra contract columns not in DDL: {sorted(extra_cols)}")

        for table in contract_tables:
            if table not in ddl_tables:
                errors.append(f"Orphan source contract {system}.{table}: no matching table in DDL")

    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        sys.exit(1)

    total = sum(len(v) for v in contracts.values())
    print(f"OK: {total} source table contracts aligned with source init DDL", file=sys.stderr)


if __name__ == "__main__":
    main()
