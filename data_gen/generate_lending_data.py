"""
Generate lending synthetic rows: branches, customers, applications, loans, repayments.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Tuple

from data_gen.shared import (
    APPLICATION_STATUSES,
    CITIES,
    FIRST_NAMES,
    LAST_NAMES,
    LOAN_STATUSES,
    NUM_BRANCHES,
    NUM_LENDING_CUSTOMERS,
    NUM_LOAN_APPLICATIONS,
    NUM_LOANS,
    NUM_REPAYMENTS,
    REPAYMENT_STATUSES,
    RNG_SEED,
    fake_email,
    fake_national_id,
    fake_phone,
    random_dt_in_range,
    weighted_choice,
    window_last_months,
)


@dataclass
class LendingDataset:
    branches: List[Dict[str, Any]]
    customers: List[Dict[str, Any]]
    loan_applications: List[Dict[str, Any]]
    loans: List[Dict[str, Any]]
    repayments: List[Dict[str, Any]]


def _cohort_sizes() -> Tuple[int, int, int]:
    return 900, 200, 900  # exact, phone_name, lending_only


def build_lending_dataset() -> Tuple[LendingDataset, List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Build lending data and return payloads for paired insurance rows:
    - exact_keys: national_id, phone, full_name for exact-match cohort
    - phone_keys: phone, full_name for phone+name cohort (insurance national_id NULL)
    """
    rng = random.Random(RNG_SEED)
    start, end = window_last_months(12)

    branches: List[Dict[str, Any]] = []
    for i in range(1, NUM_BRANCHES + 1):
        branches.append(
            {
                "branch_id": i,
                "branch_name": f"Branch {chr(64 + i)} — {CITIES[(i - 1) % len(CITIES)]}",
                "city": CITIES[(i - 1) % len(CITIES)],
                "opened_at": random_dt_in_range(rng, start - timedelta(days=400), start),
            }
        )

    n_exact, n_phone, n_lend_only = _cohort_sizes()
    customers: List[Dict[str, Any]] = []
    exact_keys: List[Dict[str, str]] = []
    phone_keys: List[Dict[str, str]] = []
    customer_id = 1

    for _ in range(n_exact):
        national_id = fake_national_id(rng)
        phone = fake_phone(rng)
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        primary_branch_id = rng.randint(1, NUM_BRANCHES)
        created_at = random_dt_in_range(rng, start, end)
        customers.append(
            {
                "customer_id": customer_id,
                "national_id": national_id,
                "phone_number": phone,
                "full_name": name,
                "email": fake_email(rng, f"c{customer_id}"),
                "primary_branch_id": primary_branch_id,
                "created_at": created_at,
            }
        )
        exact_keys.append(
            {"national_id": national_id, "phone_number": phone, "full_name": name}
        )
        customer_id += 1

    for _ in range(n_phone):
        national_id = fake_national_id(rng)
        phone = fake_phone(rng)
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        primary_branch_id = rng.randint(1, NUM_BRANCHES)
        created_at = random_dt_in_range(rng, start, end)
        customers.append(
            {
                "customer_id": customer_id,
                "national_id": national_id,
                "phone_number": phone,
                "full_name": name,
                "email": fake_email(rng, f"c{customer_id}"),
                "primary_branch_id": primary_branch_id,
                "created_at": created_at,
            }
        )
        phone_keys.append({"phone_number": phone, "full_name": name})
        customer_id += 1

    for _ in range(n_lend_only):
        national_id = fake_national_id(rng)
        phone = fake_phone(rng)
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        primary_branch_id = rng.randint(1, NUM_BRANCHES)
        created_at = random_dt_in_range(rng, start, end)
        customers.append(
            {
                "customer_id": customer_id,
                "national_id": national_id,
                "phone_number": phone,
                "full_name": name,
                "email": fake_email(rng, f"c{customer_id}"),
                "primary_branch_id": primary_branch_id,
                "created_at": created_at,
            }
        )
        customer_id += 1

    assert len(customers) == NUM_LENDING_CUSTOMERS

    loan_applications: List[Dict[str, Any]] = []
    application_id = 1
    for _ in range(NUM_LOAN_APPLICATIONS):
        cust_id = rng.randint(1, NUM_LENDING_CUSTOMERS)
        branch_id = rng.randint(1, NUM_BRANCHES)
        amount = round(rng.uniform(5_000_000, 500_000_000), 2)
        status = weighted_choice(rng, APPLICATION_STATUSES, weights=(0.15, 0.65, 0.20))
        applied_at = random_dt_in_range(rng, start, end)
        loan_applications.append(
            {
                "application_id": application_id,
                "customer_id": cust_id,
                "branch_id": branch_id,
                "amount_requested": amount,
                "status": status,
                "applied_at": applied_at,
            }
        )
        application_id += 1

    loans: List[Dict[str, Any]] = []
    loan_id = 1
    approved_apps = [a for a in loan_applications if a["status"] == "approved"]
    rng.shuffle(approved_apps)
    for i in range(NUM_LOANS):
        app = approved_apps[i % len(approved_apps)]
        principal = round(
            rng.uniform(4_000_000, min(app["amount_requested"] * 1.0, 450_000_000)), 2
        )
        status = weighted_choice(rng, LOAN_STATUSES, weights=(0.72, 0.23, 0.05))
        disbursement_date = random_dt_in_range(rng, start, end).date()
        created_at = random_dt_in_range(rng, start, end)
        loans.append(
            {
                "loan_id": loan_id,
                "application_id": app["application_id"],
                "customer_id": app["customer_id"],
                "branch_id": app["branch_id"],
                "principal_amount": principal,
                "status": status,
                "disbursement_date": disbursement_date,
                "created_at": created_at,
            }
        )
        loan_id += 1

    repayments: List[Dict[str, Any]] = []
    repayment_id = 1
    for _ in range(NUM_REPAYMENTS):
        ln = rng.choice(loans)
        amt = round(rng.uniform(100_000, min(ln["principal_amount"] / 8, 50_000_000)), 2)
        paid_at = random_dt_in_range(rng, start, end)
        status = weighted_choice(rng, REPAYMENT_STATUSES, weights=(0.94, 0.06))
        repayments.append(
            {
                "repayment_id": repayment_id,
                "loan_id": ln["loan_id"],
                "amount": amt,
                "paid_at": paid_at,
                "status": status,
            }
        )
        repayment_id += 1

    return (
        LendingDataset(
            branches=branches,
            customers=customers,
            loan_applications=loan_applications,
            loans=loans,
            repayments=repayments,
        ),
        exact_keys,
        phone_keys,
    )
