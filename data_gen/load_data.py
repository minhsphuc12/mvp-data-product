"""
Truncate operational sources, load synthetic data, mirror into analytics raw schemas.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
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


def truncate_analytics_raw(cur) -> None:
    cur.execute(
        """
        TRUNCATE TABLE
            raw_lending.repayments,
            raw_lending.loans,
            raw_lending.loan_applications,
            raw_lending.customers,
            raw_lending.branches,
            raw_insurance.claims,
            raw_insurance.policies,
            raw_insurance.policy_holders
        RESTART IDENTITY CASCADE;
        """
    )


def insert_branches(cur, rows: List[Dict[str, Any]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO branches (branch_id, branch_name, city, opened_at)
        VALUES (%(branch_id)s, %(branch_name)s, %(city)s, %(opened_at)s);
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
        );
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
        );
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


def mirror_lending_raw(cur, lending, loaded_at: datetime) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO raw_lending.branches (branch_id, branch_name, city, opened_at, loaded_at)
        VALUES (%(branch_id)s, %(branch_name)s, %(city)s, %(opened_at)s, %(loaded_at)s);
        """,
        [{**r, "loaded_at": loaded_at} for r in lending.branches],
    )
    execute_batch(
        cur,
        """
        INSERT INTO raw_lending.customers (
            customer_id, national_id, phone_number, full_name, email,
            primary_branch_id, created_at, loaded_at
        )
        VALUES (
            %(customer_id)s, %(national_id)s, %(phone_number)s, %(full_name)s, %(email)s,
            %(primary_branch_id)s, %(created_at)s, %(loaded_at)s
        );
        """,
        [{**r, "loaded_at": loaded_at} for r in lending.customers],
    )
    execute_batch(
        cur,
        """
        INSERT INTO raw_lending.loan_applications (
            application_id, customer_id, branch_id, amount_requested, status, applied_at, loaded_at
        )
        VALUES (
            %(application_id)s, %(customer_id)s, %(branch_id)s, %(amount_requested)s,
            %(status)s, %(applied_at)s, %(loaded_at)s
        );
        """,
        [{**r, "loaded_at": loaded_at} for r in lending.loan_applications],
    )
    execute_batch(
        cur,
        """
        INSERT INTO raw_lending.loans (
            loan_id, application_id, customer_id, branch_id, principal_amount,
            status, disbursement_date, created_at, loaded_at
        )
        VALUES (
            %(loan_id)s, %(application_id)s, %(customer_id)s, %(branch_id)s,
            %(principal_amount)s, %(status)s, %(disbursement_date)s, %(created_at)s, %(loaded_at)s
        );
        """,
        [{**r, "loaded_at": loaded_at} for r in lending.loans],
    )
    execute_batch(
        cur,
        """
        INSERT INTO raw_lending.repayments (
            repayment_id, loan_id, amount, paid_at, status, loaded_at
        )
        VALUES (
            %(repayment_id)s, %(loan_id)s, %(amount)s, %(paid_at)s, %(status)s, %(loaded_at)s
        );
        """,
        [{**r, "loaded_at": loaded_at} for r in lending.repayments],
    )


def mirror_insurance_raw(cur, insurance, loaded_at: datetime) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO raw_insurance.policy_holders (
            policy_holder_id, national_id, phone_number, full_name, email, created_at, loaded_at
        )
        VALUES (
            %(policy_holder_id)s, %(national_id)s, %(phone_number)s, %(full_name)s,
            %(email)s, %(created_at)s, %(loaded_at)s
        );
        """,
        [{**r, "loaded_at": loaded_at} for r in insurance.policy_holders],
    )
    execute_batch(
        cur,
        """
        INSERT INTO raw_insurance.policies (
            policy_id, policy_holder_id, policy_number, product_type, premium_amount,
            coverage_start_date, coverage_end_date, status, loaded_at
        )
        VALUES (
            %(policy_id)s, %(policy_holder_id)s, %(policy_number)s, %(product_type)s,
            %(premium_amount)s, %(coverage_start_date)s, %(coverage_end_date)s, %(status)s,
            %(loaded_at)s
        );
        """,
        [{**r, "loaded_at": loaded_at} for r in insurance.policies],
    )
    execute_batch(
        cur,
        """
        INSERT INTO raw_insurance.claims (
            claim_id, policy_id, claim_amount, status, filed_at, settled_at, loaded_at
        )
        VALUES (
            %(claim_id)s, %(policy_id)s, %(claim_amount)s, %(status)s,
            %(filed_at)s, %(settled_at)s, %(loaded_at)s
        );
        """,
        [{**r, "loaded_at": loaded_at} for r in insurance.claims],
    )


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

                print("Truncating analytics raw layers...")
                truncate_analytics_raw(ca)
                print("Mirroring into analytics_db raw_lending / raw_insurance...")
                mirror_lending_raw(ca, lending, loaded_at)
                mirror_insurance_raw(ca, insurance, loaded_at)

        print("Done. Rows loaded into sources and analytics raw schemas.")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
