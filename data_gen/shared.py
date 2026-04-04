"""
Shared utilities for deterministic synthetic data (overlap, dates, normalization).
"""
from __future__ import annotations

import random
import re
import string
from datetime import datetime, timedelta, timezone
from typing import Final, Optional, Tuple

# Volume targets (per spec)
NUM_BRANCHES: Final[int] = 5
NUM_LENDING_CUSTOMERS: Final[int] = 2000
NUM_POLICY_HOLDERS: Final[int] = 2000
NUM_LOAN_APPLICATIONS: Final[int] = 3000
NUM_LOANS: Final[int] = 2000
NUM_REPAYMENTS: Final[int] = 8000
NUM_POLICIES: Final[int] = 1500
NUM_CLAIMS: Final[int] = 300

# Overlap cohorts
COHORT_EXACT_MATCH: Final[int] = 900
COHORT_PHONE_NAME_MATCH: Final[int] = 200
COHORT_LENDING_ONLY: Final[int] = 900
COHORT_INSURANCE_ONLY: Final[int] = 900

RNG_SEED: Final[int] = 42

FIRST_NAMES: Final[Tuple[str, ...]] = (
    "An", "Binh", "Chi", "Dung", "Giang", "Hoa", "Hung", "Lan", "Linh", "Long",
    "Mai", "Minh", "Nam", "Ngoc", "Phong", "Quan", "Son", "Tam", "Thao", "Tuan",
    "Van", "Viet", "Yen", "Hieu", "Khanh",
)
LAST_NAMES: Final[Tuple[str, ...]] = (
    "Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Vo", "Dang", "Bui", "Do",
    "Ngo", "Duong", "Ly", "Truong", "Phan", "Ho", "Dinh", "Dao", "Mai", "Ton",
)
CITIES: Final[Tuple[str, ...]] = ("Hanoi", "Ho Chi Minh City", "Da Nang", "Can Tho", "Hai Phong")
PRODUCT_TYPES: Final[Tuple[str, ...]] = ("motor", "health", "property", "travel", "life")

APPLICATION_STATUSES: Final[Tuple[str, ...]] = ("pending", "approved", "rejected")
LOAN_STATUSES: Final[Tuple[str, ...]] = ("active", "closed", "defaulted")
REPAYMENT_STATUSES: Final[Tuple[str, ...]] = ("completed", "failed")
POLICY_STATUSES: Final[Tuple[str, ...]] = ("active", "lapsed", "cancelled")
CLAIM_STATUSES: Final[Tuple[str, ...]] = ("open", "approved", "denied", "closed")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def random_dt_in_range(rng: random.Random, start: datetime, end: datetime) -> datetime:
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=rng.uniform(0, delta))


def window_last_months(months: int, end: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    end = end or utc_now()
    start = end - timedelta(days=30 * months)
    return start, end


def normalize_full_name(name: str) -> str:
    """Uppercase, collapse whitespace — mirrors dbt staging normalization."""
    s = name.upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def fake_national_id(rng: random.Random) -> str:
    return "".join(rng.choice(string.digits) for _ in range(12))


def fake_phone(rng: random.Random) -> str:
    return "0" + "".join(rng.choice(string.digits) for _ in range(9))


def fake_email(rng: random.Random, prefix: str) -> str:
    domain = rng.choice(("mail.example", "inbox.demo", "contact.finance"))
    return f"{prefix}.{rng.randint(1000, 9999)}@{domain}.vn"


def weighted_choice(rng: random.Random, choices: Tuple[str, ...], weights: Tuple[float, ...]) -> str:
    return rng.choices(choices, weights=weights, k=1)[0]
