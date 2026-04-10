"""
Validate that synthetic seed data contains time-varying snapshots for SCD2 demos.
"""
from __future__ import annotations

import os
import sys
from typing import Sequence

import psycopg2


def _require_env(keys: Sequence[str]) -> None:
    missing = [k for k in keys if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")


def _connect_analytics():
    _require_env(
        (
            "ANALYTICS_DB_HOST",
            "ANALYTICS_DB_PORT",
            "ANALYTICS_DB_USER",
            "ANALYTICS_DB_PASSWORD",
            "ANALYTICS_DB_DATABASE",
        )
    )
    return psycopg2.connect(
        host=os.environ["ANALYTICS_DB_HOST"],
        port=os.environ["ANALYTICS_DB_PORT"],
        user=os.environ["ANALYTICS_DB_USER"],
        password=os.environ["ANALYTICS_DB_PASSWORD"],
        dbname=os.environ["ANALYTICS_DB_DATABASE"],
    )


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    try:
        with _connect_analytics() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select count(distinct loaded_at) as batch_count
                from staging.lending_customers
                """
            )
            customer_batch_count = cur.fetchone()[0]

            cur.execute(
                """
                select count(distinct loaded_at) as batch_count
                from staging.lending_branches
                """
            )
            branch_batch_count = cur.fetchone()[0]

            cur.execute(
                """
                with c as (
                    select customer_id, count(*) as version_count
                    from staging.lending_customers
                    group by customer_id
                ),
                b as (
                    select branch_id, count(*) as version_count
                    from staging.lending_branches
                    group by branch_id
                )
                select
                    (select max(version_count) from c) as customer_max_versions,
                    (select max(version_count) from b) as branch_max_versions
                """
            )
            customer_max_versions, branch_max_versions = cur.fetchone()

        print(f"staging.lending_customers distinct loaded_at: {customer_batch_count}")
        print(f"staging.lending_branches distinct loaded_at: {branch_batch_count}")
        print(f"staging.lending_customers max versions per customer_id: {customer_max_versions}")
        print(f"staging.lending_branches max versions per branch_id: {branch_max_versions}")

        if customer_batch_count < 3 or branch_batch_count < 3:
            print("ERROR: expected at least 3 time batches in raw snapshots.", file=sys.stderr)
            return 1

        if customer_max_versions < 2 or branch_max_versions < 2:
            print("ERROR: expected at least one entity with multiple versions.", file=sys.stderr)
            return 1

        print("SCD2 seed history validation passed.")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
