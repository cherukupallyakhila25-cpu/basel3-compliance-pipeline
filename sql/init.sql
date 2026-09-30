-- =====================================================================
-- Basel III Compliance Pipeline: database schema
-- Runs automatically the FIRST time the postgres container starts.
-- =====================================================================

-- 1) RISK WEIGHTS: how "risky" each asset class is (Basel III standardised
--    approach, simplified). RWA = exposure x risk_weight. Spark will
--    BROADCAST-JOIN this small table onto millions of loans in Part 2.
CREATE TABLE IF NOT EXISTS risk_weight_config (
    asset_class   VARCHAR(50) PRIMARY KEY,
    risk_weight   NUMERIC(6,4) NOT NULL,
    description   TEXT
);

INSERT INTO risk_weight_config (asset_class, risk_weight, description) VALUES
    ('CASH',                   0.0000, 'Cash and central bank reserves'),
    ('SOVEREIGN_BOND',         0.0000, 'Highly rated government bonds'),
    ('BANK_EXPOSURE',          0.2000, 'Claims on highly rated banks'),
    ('RESIDENTIAL_MORTGAGE',   0.3500, 'Loans secured by residential property'),
    ('RETAIL',                 0.7500, 'Regulatory retail portfolio'),
    ('CORPORATE',              1.0000, 'Unrated corporate loans'),
    ('CORPORATE_BOND',         1.0000, 'Corporate debt securities'),
    ('COMMERCIAL_REAL_ESTATE', 1.0000, 'Commercial real estate loans'),
    ('PAST_DUE',               1.5000, 'Any loan 90+ days past due')
ON CONFLICT (asset_class) DO NOTHING;           -- safe to run twice

-- 2) HQLA FACTORS: what share of a liquid asset's market value counts
--    toward the LCR numerator (1 - haircut).
CREATE TABLE IF NOT EXISTS hqla_config (
    hqla_level   VARCHAR(20) PRIMARY KEY,
    hqla_factor  NUMERIC(5,4) NOT NULL,
    description  TEXT
);

INSERT INTO hqla_config (hqla_level, hqla_factor, description) VALUES
    ('LEVEL_1',  1.0000, 'Cash, reserves, sovereign bonds: no haircut'),
    ('LEVEL_2A', 0.8500, 'High-grade corporate bonds: 15% haircut'),
    ('LEVEL_2B', 0.5000, 'Lower-grade securities: 50% haircut'),
    ('NON_HQLA', 0.0000, 'Not liquid enough to count')
ON CONFLICT (hqla_level) DO NOTHING;

-- 3) CASH-FLOW RATES: stress assumptions for the LCR denominator.
--    Outflow rate = share of a deposit type assumed to run off in a crisis.
CREATE TABLE IF NOT EXISTS cash_flow_rate_config (
    category     VARCHAR(50) PRIMARY KEY,
    direction    VARCHAR(10) NOT NULL CHECK (direction IN ('OUTFLOW', 'INFLOW')),
    rate         NUMERIC(5,4) NOT NULL,
    description  TEXT
);

INSERT INTO cash_flow_rate_config (category, direction, rate, description) VALUES
    ('RETAIL_STABLE',             'OUTFLOW', 0.0500, 'Insured retail deposits'),
    ('RETAIL_LESS_STABLE',        'OUTFLOW', 0.1000, 'Uninsured retail deposits'),
    ('OPERATIONAL',               'OUTFLOW', 0.2500, 'Operational corporate deposits'),
    ('CORPORATE_NON_OPERATIONAL', 'OUTFLOW', 0.4000, 'Non-operational corporate deposits'),
    ('FINANCIAL_INSTITUTION',     'OUTFLOW', 1.0000, 'Wholesale funding from other banks'),
    ('RETAIL_LOAN_REPAYMENT',     'INFLOW',  0.5000, 'Contractual repayments from performing loans')
ON CONFLICT (category) DO NOTHING;

-- 4) REGULATORY METRICS: the main output table. One row per bank (or branch)
--    per metric per reporting date.
CREATE TABLE IF NOT EXISTS regulatory_metrics (
    id               SERIAL PRIMARY KEY,
    run_id           VARCHAR(50)   NOT NULL,     -- which pipeline run produced it
    report_date      DATE          NOT NULL,
    bank_id          VARCHAR(20)   NOT NULL,
    branch_id        VARCHAR(30),                -- NULL = bank-level figure
    metric_name      VARCHAR(10)   NOT NULL CHECK (metric_name IN ('CAR', 'LCR', 'NPL')),
    numerator        NUMERIC(20,2),              -- e.g. Tier 1 capital
    denominator      NUMERIC(20,2),              -- e.g. RWA
    metric_value     NUMERIC(12,6) NOT NULL,     -- numerator / denominator
    regulatory_limit NUMERIC(12,6),
    is_compliant     BOOLEAN,
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_bank_date
    ON regulatory_metrics (bank_id, report_date, metric_name);

-- 5) FDIC BENCHMARKS: US industry averages from the FDIC API.
CREATE TABLE IF NOT EXISTS fdic_benchmarks (
    report_date      DATE        NOT NULL,
    metric_name      VARCHAR(20) NOT NULL,
    industry_mean    NUMERIC(12,6),
    industry_median  NUMERIC(12,6),
    bank_count       INTEGER,
    loaded_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_date, metric_name)
);

-- 6) AUDIT LOG: every pipeline step writes here. Regulators require a trail.
CREATE TABLE IF NOT EXISTS audit_log (
    id                SERIAL PRIMARY KEY,
    run_id            VARCHAR(50),
    task_name         VARCHAR(100) NOT NULL,
    status            VARCHAR(20)  NOT NULL CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED')),
    records_processed BIGINT,
    message           TEXT,
    logged_at         TIMESTAMP DEFAULT NOW()
);
