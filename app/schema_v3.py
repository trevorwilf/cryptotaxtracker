"""
Tax computation schema — v3 additions.

These tables are populated by the tax computation pipeline:
  1. TransferMatcher  → tax.transfer_matches
  2. IncomeClassifier → tax.income_events
  3. TaxEngine        → tax.lots, tax.disposals, tax.form_8949

Import and call ensure_tax_tables(engine) during app startup.
"""

SCHEMA_V3_SQL = """
-- Transfer matches (cross-exchange withdrawal→deposit pairs)
CREATE TABLE IF NOT EXISTS tax.transfer_matches (
    id              SERIAL PRIMARY KEY,
    withdrawal_id   INTEGER REFERENCES tax.withdrawals(id),
    deposit_id      INTEGER REFERENCES tax.deposits(id),
    asset           VARCHAR(50) NOT NULL,
    amount          NUMERIC(36,18),
    from_exchange   VARCHAR(50),
    to_exchange     VARCHAR(50),
    transferred_at  TIMESTAMPTZ,
    tx_hash         VARCHAR(500),
    match_confidence VARCHAR(20),
    cost_basis_usd  NUMERIC(36,18),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tm_withdrawal ON tax.transfer_matches(withdrawal_id);
CREATE INDEX IF NOT EXISTS idx_tm_deposit ON tax.transfer_matches(deposit_id);

-- Income events (staking, airdrops, pool rewards — ordinary income)
CREATE TABLE IF NOT EXISTS tax.income_events (
    id              SERIAL PRIMARY KEY,
    exchange        VARCHAR(50) NOT NULL,
    asset           VARCHAR(50) NOT NULL,
    amount          NUMERIC(36,18) NOT NULL,
    amount_usd      NUMERIC(36,18),
    income_type     VARCHAR(30) NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL,
    description     TEXT,
    deposit_id      INTEGER,
    pool_activity_id INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_income_type ON tax.income_events(income_type, received_at);

-- Acquisition lots (FIFO cost basis tracking)
CREATE TABLE IF NOT EXISTS tax.lots (
    id              SERIAL PRIMARY KEY,
    asset           VARCHAR(50)    NOT NULL,
    quantity        NUMERIC(36,18) NOT NULL,
    remaining       NUMERIC(36,18) NOT NULL,
    cost_per_unit_usd NUMERIC(36,18),
    total_cost_usd  NUMERIC(36,18),
    acquired_at     TIMESTAMPTZ    NOT NULL,
    exchange        VARCHAR(50),
    source          VARCHAR(30),
    source_trade_id INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(asset, exchange, acquired_at, source_trade_id)
);
CREATE INDEX IF NOT EXISTS idx_lots_asset ON tax.lots(asset, acquired_at);

-- Disposals (sell-side matched to lots)
CREATE TABLE IF NOT EXISTS tax.disposals (
    id              SERIAL PRIMARY KEY,
    asset           VARCHAR(50)    NOT NULL,
    quantity        NUMERIC(36,18) NOT NULL,
    proceeds_usd    NUMERIC(36,18),
    cost_basis_usd  NUMERIC(36,18),
    gain_loss_usd   NUMERIC(36,18),
    fee_usd         NUMERIC(36,18),
    acquired_at     TIMESTAMPTZ,
    disposed_at     TIMESTAMPTZ    NOT NULL,
    holding_days    INTEGER,
    term            VARCHAR(10),
    exchange        VARCHAR(50),
    market          VARCHAR(100),
    lot_id          INTEGER,
    trade_id        INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_disposals_date ON tax.disposals(disposed_at);
CREATE INDEX IF NOT EXISTS idx_disposals_term ON tax.disposals(term);

-- Form 8949 lines (ready for Schedule D)
CREATE TABLE IF NOT EXISTS tax.form_8949 (
    id              SERIAL PRIMARY KEY,
    description     TEXT,
    date_acquired   VARCHAR(20),
    date_sold       VARCHAR(20),
    proceeds        NUMERIC(36,2),
    cost_basis      NUMERIC(36,2),
    adjustment_code VARCHAR(10),
    adjustment_amount NUMERIC(36,2),
    gain_loss       NUMERIC(36,2),
    term            VARCHAR(10),
    box             VARCHAR(5),
    asset           VARCHAR(50),
    exchange        VARCHAR(50),
    holding_days    INTEGER,
    is_futures      BOOLEAN DEFAULT FALSE,
    tax_year        INTEGER NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_f8949_year ON tax.form_8949(tax_year);
CREATE INDEX IF NOT EXISTS idx_f8949_box ON tax.form_8949(box, tax_year);
"""
