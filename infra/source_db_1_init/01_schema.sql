-- Lending operational schema (source_db_1)
CREATE TABLE IF NOT EXISTS branches (
    branch_id       SERIAL PRIMARY KEY,
    branch_name     VARCHAR(120) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id         SERIAL PRIMARY KEY,
    national_id         VARCHAR(32),
    phone_number        VARCHAR(32) NOT NULL,
    full_name           VARCHAR(200) NOT NULL,
    email               VARCHAR(200),
    primary_branch_id   INTEGER NOT NULL REFERENCES branches (branch_id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_customers_national_id ON customers (national_id);
CREATE INDEX idx_customers_phone ON customers (phone_number);

CREATE TABLE IF NOT EXISTS loan_applications (
    application_id      SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL REFERENCES customers (customer_id),
    branch_id           INTEGER NOT NULL REFERENCES branches (branch_id),
    amount_requested    NUMERIC(14, 2) NOT NULL,
    status              VARCHAR(32) NOT NULL,
    applied_at          TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_loan_applications_customer ON loan_applications (customer_id);

CREATE TABLE IF NOT EXISTS loans (
    loan_id             SERIAL PRIMARY KEY,
    application_id      INTEGER REFERENCES loan_applications (application_id),
    customer_id         INTEGER NOT NULL REFERENCES customers (customer_id),
    branch_id           INTEGER NOT NULL REFERENCES branches (branch_id),
    principal_amount    NUMERIC(14, 2) NOT NULL,
    status              VARCHAR(32) NOT NULL,
    disbursement_date   DATE NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_loans_customer ON loans (customer_id);
CREATE INDEX idx_loans_branch ON loans (branch_id);

CREATE TABLE IF NOT EXISTS repayments (
    repayment_id    SERIAL PRIMARY KEY,
    loan_id         INTEGER NOT NULL REFERENCES loans (loan_id),
    amount          NUMERIC(14, 2) NOT NULL,
    paid_at         TIMESTAMPTZ NOT NULL,
    status          VARCHAR(32) NOT NULL
);

CREATE INDEX idx_repayments_loan ON repayments (loan_id);
