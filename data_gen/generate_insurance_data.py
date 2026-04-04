"""
Generate insurance synthetic rows paired with lending overlap cohorts.
"""
from __future__ import annotations

import random
import string
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List

from data_gen.shared import (
    CLAIM_STATUSES,
    FIRST_NAMES,
    LAST_NAMES,
    NUM_CLAIMS,
    NUM_POLICIES,
    NUM_POLICY_HOLDERS,
    POLICY_STATUSES,
    PRODUCT_TYPES,
    RNG_SEED,
    fake_email,
    fake_national_id,
    fake_phone,
    random_dt_in_range,
    weighted_choice,
    window_last_months,
)


@dataclass
class InsuranceDataset:
    policy_holders: List[Dict[str, Any]]
    policies: List[Dict[str, Any]]
    claims: List[Dict[str, Any]]


def _random_policy_number(rng: random.Random, idx: int) -> str:
    suffix = "".join(rng.choice(string.ascii_uppercase + string.digits) for _ in range(6))
    return f"POL-{idx:05d}-{suffix}"


def build_insurance_dataset(
    exact_keys: List[Dict[str, str]],
    phone_keys: List[Dict[str, str]],
) -> InsuranceDataset:
    """
    Build 2000 policy holders: same 900 exact + 200 phone/name + 900 insurance-only.
    """
    rng = random.Random(RNG_SEED + 7)
    start, end = window_last_months(12)

    policy_holders: List[Dict[str, Any]] = []
    holder_id = 1

    for k in exact_keys:
        created_at = random_dt_in_range(rng, start, end)
        policy_holders.append(
            {
                "policy_holder_id": holder_id,
                "national_id": k["national_id"],
                "phone_number": k["phone_number"],
                "full_name": k["full_name"],
                "email": fake_email(rng, f"ph{holder_id}"),
                "created_at": created_at,
            }
        )
        holder_id += 1

    for k in phone_keys:
        created_at = random_dt_in_range(rng, start, end)
        policy_holders.append(
            {
                "policy_holder_id": holder_id,
                "national_id": None,
                "phone_number": k["phone_number"],
                "full_name": k["full_name"],
                "email": fake_email(rng, f"ph{holder_id}"),
                "created_at": created_at,
            }
        )
        holder_id += 1

    n_ins_only = NUM_POLICY_HOLDERS - len(policy_holders)
    assert n_ins_only == 900

    for _ in range(n_ins_only):
        national_id = fake_national_id(rng)
        phone = fake_phone(rng)
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        created_at = random_dt_in_range(rng, start, end)
        policy_holders.append(
            {
                "policy_holder_id": holder_id,
                "national_id": national_id,
                "phone_number": phone,
                "full_name": name,
                "email": fake_email(rng, f"ph{holder_id}"),
                "created_at": created_at,
            }
        )
        holder_id += 1

    assert len(policy_holders) == NUM_POLICY_HOLDERS

    policies: List[Dict[str, Any]] = []
    policy_id = 1
    holders_shuffled = list(range(1, NUM_POLICY_HOLDERS + 1))
    rng.shuffle(holders_shuffled)
    for _ in range(NUM_POLICIES):
        hid = holders_shuffled[policy_id % NUM_POLICY_HOLDERS]
        product = rng.choice(PRODUCT_TYPES)
        premium = round(rng.uniform(500_000, 20_000_000), 2)
        cov_start = random_dt_in_range(rng, start, end).date()
        cov_end = cov_start + timedelta(days=rng.randint(180, 730))
        status = weighted_choice(rng, POLICY_STATUSES, weights=(0.78, 0.12, 0.10))
        policies.append(
            {
                "policy_id": policy_id,
                "policy_holder_id": hid,
                "policy_number": _random_policy_number(rng, policy_id),
                "product_type": product,
                "premium_amount": premium,
                "coverage_start_date": cov_start,
                "coverage_end_date": cov_end,
                "status": status,
            }
        )
        policy_id += 1

    claims: List[Dict[str, Any]] = []
    claim_id = 1
    for _ in range(NUM_CLAIMS):
        pol = rng.choice(policies)
        claim_amount = round(rng.uniform(1_000_000, 80_000_000), 2)
        status = weighted_choice(rng, CLAIM_STATUSES, weights=(0.18, 0.42, 0.15, 0.25))
        filed_at = random_dt_in_range(rng, start, end)
        settled_at = (
            random_dt_in_range(rng, filed_at, end) if status in ("approved", "closed", "denied") else None
        )
        claims.append(
            {
                "claim_id": claim_id,
                "policy_id": pol["policy_id"],
                "claim_amount": claim_amount,
                "status": status,
                "filed_at": filed_at,
                "settled_at": settled_at,
            }
        )
        claim_id += 1

    return InsuranceDataset(
        policy_holders=policy_holders,
        policies=policies,
        claims=claims,
    )
