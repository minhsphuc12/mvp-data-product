-- Analytics warehouse: raw landing zones (mirrored from operational DBs via Python)
CREATE SCHEMA IF NOT EXISTS raw_lending;
CREATE SCHEMA IF NOT EXISTS raw_insurance;

-- Lending raw tables (same shape as source_db_1)
CREATE TABLE IF NOT EXISTS raw_lending.branches (
    branch_id       INTEGER NOT NULL,
    branch_name     VARCHAR(120) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    opened_at       TIMESTAMPTZ NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (branch_id, loaded_at)
);

CREATE TABLE IF NOT EXISTS raw_lending.customers (
    customer_id         INTEGER NOT NULL,
    national_id         VARCHAR(32),
    phone_number        VARCHAR(32) NOT NULL,
    full_name           VARCHAR(200) NOT NULL,
    email               VARCHAR(200),
    primary_branch_id   INTEGER NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (customer_id, loaded_at)
);

CREATE TABLE IF NOT EXISTS raw_lending.loan_applications (
    application_id      INTEGER PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    branch_id           INTEGER NOT NULL,
    amount_requested    NUMERIC(14, 2) NOT NULL,
    status              VARCHAR(32) NOT NULL,
    applied_at          TIMESTAMPTZ NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_lending.loans (
    loan_id             INTEGER PRIMARY KEY,
    application_id      INTEGER,
    customer_id         INTEGER NOT NULL,
    branch_id           INTEGER NOT NULL,
    principal_amount    NUMERIC(14, 2) NOT NULL,
    status              VARCHAR(32) NOT NULL,
    disbursement_date   DATE NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_lending.repayments (
    repayment_id    INTEGER PRIMARY KEY,
    loan_id         INTEGER NOT NULL,
    amount          NUMERIC(14, 2) NOT NULL,
    paid_at         TIMESTAMPTZ NOT NULL,
    status          VARCHAR(32) NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Insurance raw tables (same shape as source_db_2)
CREATE TABLE IF NOT EXISTS raw_insurance.policy_holders (
    policy_holder_id    INTEGER NOT NULL,
    national_id         VARCHAR(32),
    phone_number        VARCHAR(32) NOT NULL,
    full_name           VARCHAR(200) NOT NULL,
    email               VARCHAR(200),
    created_at          TIMESTAMPTZ NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (policy_holder_id, loaded_at)
);

CREATE TABLE IF NOT EXISTS raw_insurance.policies (
    policy_id               INTEGER PRIMARY KEY,
    policy_holder_id        INTEGER NOT NULL,
    policy_number           VARCHAR(64) NOT NULL,
    product_type            VARCHAR(64) NOT NULL,
    premium_amount          NUMERIC(14, 2) NOT NULL,
    coverage_start_date     DATE NOT NULL,
    coverage_end_date       DATE NOT NULL,
    status                  VARCHAR(32) NOT NULL,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_insurance.claims (
    claim_id        INTEGER PRIMARY KEY,
    policy_id       INTEGER NOT NULL,
    claim_amount    NUMERIC(14, 2) NOT NULL,
    status          VARCHAR(32) NOT NULL,
    filed_at        TIMESTAMPTZ NOT NULL,
    settled_at      TIMESTAMPTZ,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- dbt-managed schemas (created explicitly for clarity; dbt can also create on run)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
