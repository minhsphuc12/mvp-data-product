-- Insurance operational schema (source_db_2)
CREATE TABLE IF NOT EXISTS policy_holders (
    policy_holder_id    SERIAL PRIMARY KEY,
    national_id         VARCHAR(32),
    phone_number        VARCHAR(32) NOT NULL,
    full_name           VARCHAR(200) NOT NULL,
    email               VARCHAR(200),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_policy_holders_national_id ON policy_holders (national_id);
CREATE INDEX idx_policy_holders_phone ON policy_holders (phone_number);

CREATE TABLE IF NOT EXISTS policies (
    policy_id               SERIAL PRIMARY KEY,
    policy_holder_id        INTEGER NOT NULL REFERENCES policy_holders (policy_holder_id),
    policy_number           VARCHAR(64) NOT NULL UNIQUE,
    product_type            VARCHAR(64) NOT NULL,
    premium_amount          NUMERIC(14, 2) NOT NULL,
    coverage_start_date     DATE NOT NULL,
    coverage_end_date       DATE NOT NULL,
    status                  VARCHAR(32) NOT NULL
);

CREATE INDEX idx_policies_holder ON policies (policy_holder_id);

CREATE TABLE IF NOT EXISTS claims (
    claim_id        SERIAL PRIMARY KEY,
    policy_id       INTEGER NOT NULL REFERENCES policies (policy_id),
    claim_amount    NUMERIC(14, 2) NOT NULL,
    status          VARCHAR(32) NOT NULL,
    filed_at        TIMESTAMPTZ NOT NULL,
    settled_at      TIMESTAMPTZ
);

CREATE INDEX idx_claims_policy ON claims (policy_id);
