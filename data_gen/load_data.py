"""
Truncate operational sources, load synthetic data, then pull directly into analytics staging.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Sequence

import psycopg2
from psycopg2.extras import execute_batch

from data_gen.generate_insurance_data import build_insurance_dataset
from data_gen.generate_lending_data import build_lending_dataset


def _require_env(keys: Sequence[str]) -> Dict[str, str]:
    missing = [k for k in keys if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")
    return {k: os.environ[k] for k in keys}


def _connect_from_env(prefix: str):
    env = _require_env(
        (
            f"{prefix}_HOST",
            f"{prefix}_PORT",
            f"{prefix}_USER",
            f"{prefix}_PASSWORD",
            f"{prefix}_DATABASE",
        )
    )
    return psycopg2.connect(
        host=env[f"{prefix}_HOST"],
        port=env[f"{prefix}_PORT"],
        user=env[f"{prefix}_USER"],
        password=env[f"{prefix}_PASSWORD"],
        dbname=env[f"{prefix}_DATABASE"],
    )


def _utc_loaded_at() -> datetime:
    return datetime.now(timezone.utc)


def _scd2_batch_timestamps(base_ts: datetime) -> List[datetime]:
    return [
        base_ts - timedelta(days=60),
        base_ts - timedelta(days=30),
        base_ts,
    ]


def _lending_scd2_snapshot(lending, batch_index: int):
    branches = [{**row} for row in lending.branches]
    customers = [{**row} for row in lending.customers]

    for row in branches:
        branch_id = row["branch_id"]
        if batch_index >= 1 and branch_id % 4 == 0:
            row["city"] = f"{row['city']} Metro"
        if batch_index >= 2 and branch_id % 5 == 0:
            row["branch_name"] = f"{row['branch_name']} Hub"

    for row in customers:
        customer_id = row["customer_id"]
        if batch_index >= 1 and customer_id % 7 == 0:
            row["phone_number"] = f"{row['phone_number'][:-2]}77"
        if batch_index >= 1 and customer_id % 11 == 0:
            row["primary_branch_id"] = (row["primary_branch_id"] % len(branches)) + 1
        if batch_index >= 2 and customer_id % 13 == 0:
            row["email"] = f"customer{customer_id}@changes.example.com"
        if batch_index >= 2 and customer_id % 17 == 0:
            row["full_name"] = f"{row['full_name']} Jr"

    return branches, customers


def _insurance_scd2_snapshot(insurance, batch_index: int):
    policy_holders = [{**row} for row in insurance.policy_holders]
    for row in policy_holders:
        holder_id = row["policy_holder_id"]
        if batch_index >= 1 and holder_id % 7 == 0:
            row["phone_number"] = f"{row['phone_number'][:-2]}77"
        if batch_index >= 2 and holder_id % 13 == 0:
            row["email"] = f"holder{holder_id}@changes.example.com"
        if batch_index >= 2 and holder_id % 17 == 0:
            row["full_name"] = f"{row['full_name']} Jr"
    return policy_holders


def truncate_lending_source(cur) -> None:
    cur.execute(
        """
        TRUNCATE TABLE
            repayments,
            loans,
            loan_applications,
            customers,
            branches
        RESTART IDENTITY CASCADE;
        """
    )


def truncate_insurance_source(cur) -> None:
    cur.execute(
        """
        TRUNCATE TABLE
            claims,
            policies,
            policy_holders
        RESTART IDENTITY CASCADE;
        """
    )


def truncate_analytics_staging(cur) -> None:
    cur.execute(
        """
        TRUNCATE TABLE
            staging.lending_repayments,
            staging.lending_loans,
            staging.lending_loan_applications,
            staging.lending_customers,
            staging.lending_branches,
            staging.insurance_claims,
            staging.insurance_policies,
            staging.insurance_policy_holders
        RESTART IDENTITY CASCADE;
        """
    )


def ensure_analytics_staging_schema(cur) -> None:
    cur.execute(
        """
        create schema if not exists staging;

        create table if not exists staging.lending_branches (
            branch_id integer not null,
            branch_name varchar(120) not null,
            city varchar(100) not null,
            opened_at timestamptz not null,
            loaded_at timestamptz not null default now(),
            primary key (branch_id, loaded_at)
        );

        create table if not exists staging.lending_customers (
            customer_id integer not null,
            national_id varchar(32),
            phone_number varchar(32) not null,
            full_name varchar(200) not null,
            email varchar(200),
            primary_branch_id integer not null,
            created_at timestamptz not null,
            loaded_at timestamptz not null default now(),
            primary key (customer_id, loaded_at)
        );

        create table if not exists staging.lending_loan_applications (
            application_id integer primary key,
            customer_id integer not null,
            branch_id integer not null,
            amount_requested numeric(14, 2) not null,
            status varchar(32) not null,
            applied_at timestamptz not null,
            loaded_at timestamptz not null default now()
        );

        create table if not exists staging.lending_loans (
            loan_id integer primary key,
            application_id integer,
            customer_id integer not null,
            branch_id integer not null,
            principal_amount numeric(14, 2) not null,
            status varchar(32) not null,
            disbursement_date date not null,
            created_at timestamptz not null,
            loaded_at timestamptz not null default now()
        );

        create table if not exists staging.lending_repayments (
            repayment_id integer primary key,
            loan_id integer not null,
            amount numeric(14, 2) not null,
            paid_at timestamptz not null,
            status varchar(32) not null,
            loaded_at timestamptz not null default now()
        );

        create table if not exists staging.insurance_policy_holders (
            policy_holder_id integer not null,
            national_id varchar(32),
            phone_number varchar(32) not null,
            full_name varchar(200) not null,
            email varchar(200),
            created_at timestamptz not null,
            loaded_at timestamptz not null default now(),
            primary key (policy_holder_id, loaded_at)
        );

        create table if not exists staging.insurance_policies (
            policy_id integer primary key,
            policy_holder_id integer not null,
            policy_number varchar(64) not null,
            product_type varchar(64) not null,
            premium_amount numeric(14, 2) not null,
            coverage_start_date date not null,
            coverage_end_date date not null,
            status varchar(32) not null,
            loaded_at timestamptz not null default now()
        );

        create table if not exists staging.insurance_claims (
            claim_id integer primary key,
            policy_id integer not null,
            claim_amount numeric(14, 2) not null,
            status varchar(32) not null,
            filed_at timestamptz not null,
            settled_at timestamptz,
            loaded_at timestamptz not null default now()
        );
        """
    )


def insert_branches(cur, rows: List[Dict[str, Any]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO branches (branch_id, branch_name, city, opened_at)
        VALUES (%(branch_id)s, %(branch_name)s, %(city)s, %(opened_at)s)
        ON CONFLICT (branch_id) DO UPDATE SET
            branch_name = EXCLUDED.branch_name,
            city = EXCLUDED.city,
            opened_at = EXCLUDED.opened_at;
        """,
        rows,
    )


def insert_customers(cur, rows: List[Dict[str, Any]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO customers (
            customer_id, national_id, phone_number, full_name, email,
            primary_branch_id, created_at
        )
        VALUES (
            %(customer_id)s, %(national_id)s, %(phone_number)s, %(full_name)s, %(email)s,
            %(primary_branch_id)s, %(created_at)s
        )
        ON CONFLICT (customer_id) DO UPDATE SET
            national_id = EXCLUDED.national_id,
            phone_number = EXCLUDED.phone_number,
            full_name = EXCLUDED.full_name,
            email = EXCLUDED.email,
            primary_branch_id = EXCLUDED.primary_branch_id,
            created_at = EXCLUDED.created_at;
        """,
        rows,
    )


def insert_loan_applications(cur, rows: List[Dict[str, Any]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO loan_applications (
            application_id, customer_id, branch_id, amount_requested, status, applied_at
        )
        VALUES (
            %(application_id)s, %(customer_id)s, %(branch_id)s, %(amount_requested)s,
            %(status)s, %(applied_at)s
        );
        """,
        rows,
    )


def insert_loans(cur, rows: List[Dict[str, Any]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO loans (
            loan_id, application_id, customer_id, branch_id, principal_amount,
            status, disbursement_date, created_at
        )
        VALUES (
            %(loan_id)s, %(application_id)s, %(customer_id)s, %(branch_id)s,
            %(principal_amount)s, %(status)s, %(disbursement_date)s, %(created_at)s
        );
        """,
        rows,
    )


def insert_repayments(cur, rows: List[Dict[str, Any]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO repayments (repayment_id, loan_id, amount, paid_at, status)
        VALUES (%(repayment_id)s, %(loan_id)s, %(amount)s, %(paid_at)s, %(status)s);
        """,
        rows,
    )


def insert_policy_holders(cur, rows: List[Dict[str, Any]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO policy_holders (
            policy_holder_id, national_id, phone_number, full_name, email, created_at
        )
        VALUES (
            %(policy_holder_id)s, %(national_id)s, %(phone_number)s, %(full_name)s,
            %(email)s, %(created_at)s
        )
        ON CONFLICT (policy_holder_id) DO UPDATE SET
            national_id = EXCLUDED.national_id,
            phone_number = EXCLUDED.phone_number,
            full_name = EXCLUDED.full_name,
            email = EXCLUDED.email,
            created_at = EXCLUDED.created_at;
        """,
        rows,
    )


def insert_policies(cur, rows: List[Dict[str, Any]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO policies (
            policy_id, policy_holder_id, policy_number, product_type, premium_amount,
            coverage_start_date, coverage_end_date, status
        )
        VALUES (
            %(policy_id)s, %(policy_holder_id)s, %(policy_number)s, %(product_type)s,
            %(premium_amount)s, %(coverage_start_date)s, %(coverage_end_date)s, %(status)s
        );
        """,
        rows,
    )


def insert_claims(cur, rows: List[Dict[str, Any]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO claims (
            claim_id, policy_id, claim_amount, status, filed_at, settled_at
        )
        VALUES (
            %(claim_id)s, %(policy_id)s, %(claim_amount)s, %(status)s,
            %(filed_at)s, %(settled_at)s
        );
        """,
        rows,
    )


def _fetch_rows(cur, query: str) -> List[Dict[str, Any]]:
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _append_staging(cur, insert_sql: str, rows: List[Dict[str, Any]], loaded_at: datetime) -> None:
    payload = [{**row, "loaded_at": loaded_at} for row in rows]
    if payload:
        execute_batch(cur, insert_sql, payload)


def pull_staging_scd2_snapshot(c1, c2, ca, loaded_at: datetime) -> None:
    lending_branches = _fetch_rows(c1, "select branch_id, branch_name, city, opened_at from branches")
    lending_customers = _fetch_rows(
        c1,
        """
        select customer_id, national_id, phone_number, full_name, email, primary_branch_id, created_at
        from customers
        """,
    )
    insurance_policy_holders = _fetch_rows(
        c2,
        """
        select policy_holder_id, national_id, phone_number, full_name, email, created_at
        from policy_holders
        """,
    )

    _append_staging(
        ca,
        """
        insert into staging.lending_branches (branch_id, branch_name, city, opened_at, loaded_at)
        values (%(branch_id)s, %(branch_name)s, %(city)s, %(opened_at)s, %(loaded_at)s)
        """,
        lending_branches,
        loaded_at,
    )
    _append_staging(
        ca,
        """
        insert into staging.lending_customers (
            customer_id, national_id, phone_number, full_name, email, primary_branch_id, created_at, loaded_at
        )
        values (
            %(customer_id)s, %(national_id)s, %(phone_number)s, %(full_name)s, %(email)s,
            %(primary_branch_id)s, %(created_at)s, %(loaded_at)s
        )
        """,
        lending_customers,
        loaded_at,
    )
    _append_staging(
        ca,
        """
        insert into staging.insurance_policy_holders (
            policy_holder_id, national_id, phone_number, full_name, email, created_at, loaded_at
        )
        values (
            %(policy_holder_id)s, %(national_id)s, %(phone_number)s, %(full_name)s,
            %(email)s, %(created_at)s, %(loaded_at)s
        )
        """,
        insurance_policy_holders,
        loaded_at,
    )


def pull_staging_static_tables(c1, c2, ca, loaded_at: datetime) -> None:
    lending_apps = _fetch_rows(
        c1,
        """
        select application_id, customer_id, branch_id, amount_requested, status, applied_at
        from loan_applications
        """,
    )
    lending_loans = _fetch_rows(
        c1,
        """
        select loan_id, application_id, customer_id, branch_id, principal_amount, status, disbursement_date, created_at
        from loans
        """,
    )
    lending_repayments = _fetch_rows(
        c1,
        """
        select repayment_id, loan_id, amount, paid_at, status
        from repayments
        """,
    )
    insurance_policies = _fetch_rows(
        c2,
        """
        select policy_id, policy_holder_id, policy_number, product_type, premium_amount, coverage_start_date, coverage_end_date, status
        from policies
        """,
    )
    insurance_claims = _fetch_rows(
        c2,
        """
        select claim_id, policy_id, claim_amount, status, filed_at, settled_at
        from claims
        """,
    )

    _append_staging(
        ca,
        """
        insert into staging.lending_loan_applications (
            application_id, customer_id, branch_id, amount_requested, status, applied_at, loaded_at
        ) values (
            %(application_id)s, %(customer_id)s, %(branch_id)s, %(amount_requested)s, %(status)s, %(applied_at)s, %(loaded_at)s
        )
        """,
        lending_apps,
        loaded_at,
    )
    _append_staging(
        ca,
        """
        insert into staging.lending_loans (
            loan_id, application_id, customer_id, branch_id, principal_amount, status, disbursement_date, created_at, loaded_at
        ) values (
            %(loan_id)s, %(application_id)s, %(customer_id)s, %(branch_id)s, %(principal_amount)s, %(status)s,
            %(disbursement_date)s, %(created_at)s, %(loaded_at)s
        )
        """,
        lending_loans,
        loaded_at,
    )
    _append_staging(
        ca,
        """
        insert into staging.lending_repayments (
            repayment_id, loan_id, amount, paid_at, status, loaded_at
        ) values (
            %(repayment_id)s, %(loan_id)s, %(amount)s, %(paid_at)s, %(status)s, %(loaded_at)s
        )
        """,
        lending_repayments,
        loaded_at,
    )
    _append_staging(
        ca,
        """
        insert into staging.insurance_policies (
            policy_id, policy_holder_id, policy_number, product_type, premium_amount,
            coverage_start_date, coverage_end_date, status, loaded_at
        ) values (
            %(policy_id)s, %(policy_holder_id)s, %(policy_number)s, %(product_type)s, %(premium_amount)s,
            %(coverage_start_date)s, %(coverage_end_date)s, %(status)s, %(loaded_at)s
        )
        """,
        insurance_policies,
        loaded_at,
    )
    _append_staging(
        ca,
        """
        insert into staging.insurance_claims (
            claim_id, policy_id, claim_amount, status, filed_at, settled_at, loaded_at
        ) values (
            %(claim_id)s, %(policy_id)s, %(claim_amount)s, %(status)s, %(filed_at)s, %(settled_at)s, %(loaded_at)s
        )
        """,
        insurance_claims,
        loaded_at,
    )


def apply_scd2_snapshots_to_sources_and_pull(c1, c2, ca, lending, insurance, loaded_at_list: List[datetime]) -> None:
    for batch_index, loaded_at in enumerate(loaded_at_list):
        branches, customers = _lending_scd2_snapshot(lending, batch_index)
        policy_holders = _insurance_scd2_snapshot(insurance, batch_index)

        insert_branches(c1, branches)
        insert_customers(c1, customers)
        insert_policy_holders(c2, policy_holders)
        pull_staging_scd2_snapshot(c1, c2, ca, loaded_at)


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    print("Building synthetic datasets...")
    lending, exact_keys, phone_keys = build_lending_dataset()
    insurance = build_insurance_dataset(exact_keys, phone_keys)
    loaded_at = _utc_loaded_at()
    scd2_batches = _scd2_batch_timestamps(loaded_at)

    print("Connecting to databases...")
    conn_lending = _connect_from_env("SOURCE_DB_1")
    conn_insurance = _connect_from_env("SOURCE_DB_2")
    conn_analytics = _connect_from_env("ANALYTICS_DB")

    try:
        with conn_lending, conn_insurance, conn_analytics:
            with conn_lending.cursor() as c1, conn_insurance.cursor() as c2, conn_analytics.cursor() as ca:
                print("Truncating source_db_1 (lending)...")
                truncate_lending_source(c1)
                print("Loading lending operational data...")
                insert_branches(c1, lending.branches)
                insert_customers(c1, lending.customers)
                insert_loan_applications(c1, lending.loan_applications)
                insert_loans(c1, lending.loans)
                insert_repayments(c1, lending.repayments)

                print("Truncating source_db_2 (insurance)...")
                truncate_insurance_source(c2)
                print("Loading insurance operational data...")
                insert_policy_holders(c2, insurance.policy_holders)
                insert_policies(c2, insurance.policies)
                insert_claims(c2, insurance.claims)

                ensure_analytics_staging_schema(ca)
                print("Truncating analytics staging landing tables...")
                truncate_analytics_staging(ca)

                print("Applying SCD2 snapshots and pulling from production -> analytics staging...")
                apply_scd2_snapshots_to_sources_and_pull(c1, c2, ca, lending, insurance, scd2_batches)

                print("Pulling transactional tables directly from production -> analytics staging...")
                pull_staging_static_tables(c1, c2, ca, loaded_at)

        print("Done. Rows loaded into production sources and analytics staging landing tables.")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
