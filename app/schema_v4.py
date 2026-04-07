"""
Tax computation schema — v4 (filing-grade redesign).

Addresses all 6 critical issues from the tax expert review:
  1. Double-entry trade decomposition (normalized events)
  2. Lot-slice relocation for transfers (parent_lot_id, original_acquired_at)
  3. Wallet/account-aware FIFO (wallet field on lots and events)
  4. Evidence-based classification (exception queue, no heuristic auto-classification)
  5. Audit-grade valuation (time-specific, source tracking, depeg handling)
  6. Unsupported transaction blocking (exception table with severity)

Tables:
  tax.normalized_events  — double-entry decomposition of every raw record
  tax.lots_v4            — wallet-aware lots with parent tracking
  tax.disposals_v4       — disposal-to-lot matches with full provenance
  tax.exceptions         — blocking/warning/info issues that affect filing
  tax.valuation_log      — per-event valuation audit trail
  tax.transfer_carryover — lot-slice relocation records
  tax.run_manifest       — computation run metadata for reproducibility
  tax.form_8949_v4       — Form 8949 output (year-aware, box-logic updated)
  tax.income_events_v4   — evidence-based income with manual review status
"""

SCHEMA_V4_SQL = """
-- ═══════════════════════════════════════════════════════════════════════════
-- NORMALIZED EVENT LEDGER
-- Every raw record (trade, deposit, withdrawal, pool) is decomposed into
-- one or more normalized events. A crypto-to-crypto trade produces TWO
-- events: a disposal of the given asset + an acquisition of the received.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.normalized_events (
    id                  SERIAL PRIMARY KEY,

    -- Source linkage (exactly one should be non-null)
    source_trade_id     INTEGER,
    source_deposit_id   INTEGER,
    source_withdrawal_id INTEGER,
    source_pool_id      INTEGER,

    -- Event classification
    event_type          VARCHAR(30) NOT NULL,
    -- Valid types:
    --   ACQUISITION      — received an asset (buy-side of trade, or income)
    --   DISPOSAL         — gave up an asset (sell-side of trade)
    --   FEE_DISPOSAL     — fee paid in crypto (separate taxable event)
    --   TRANSFER_OUT     — sent to own wallet (non-taxable)
    --   TRANSFER_IN      — received from own wallet (non-taxable)
    --   INCOME           — staking, reward, airdrop (ordinary income)
    --   UNSUPPORTED      — recognized but not yet supported
    --   UNRESOLVED       — needs manual classification

    -- Wallet/account context (IRS requires per-wallet basis 2025+)
    wallet              VARCHAR(100) NOT NULL,  -- exchange name or wallet address
    -- e.g., "nonkyc", "mexc", "ledger-btc-1", etc.

    -- Asset identity
    asset               VARCHAR(50) NOT NULL,
    asset_chain         VARCHAR(50),            -- e.g., "ethereum", "solana"
    asset_contract      VARCHAR(200),           -- contract address if token

    -- Quantities
    quantity            NUMERIC(36,18) NOT NULL,

    -- USD valuation (linked to valuation_log for audit trail)
    unit_price_usd      NUMERIC(36,18),
    total_usd            NUMERIC(36,18),
    valuation_id         INTEGER,               -- FK to tax.valuation_log

    -- Timestamp
    event_at            TIMESTAMPTZ NOT NULL,

    -- Pairing: for double-entry, links the two legs of a trade
    paired_event_id     INTEGER,                -- the other leg of this trade

    -- Classification metadata
    classification_rule VARCHAR(200),           -- why this classification was chosen
    manual_override     BOOLEAN DEFAULT FALSE,
    manual_notes        TEXT,

    -- Raw data preservation
    raw_market          VARCHAR(100),
    raw_side            VARCHAR(10),
    raw_data            JSONB,

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    run_id              INTEGER                 -- FK to tax.run_manifest
);
CREATE INDEX IF NOT EXISTS idx_ne_type ON tax.normalized_events(event_type);
CREATE INDEX IF NOT EXISTS idx_ne_wallet ON tax.normalized_events(wallet, asset, event_at);
CREATE INDEX IF NOT EXISTS idx_ne_source_trade ON tax.normalized_events(source_trade_id);
CREATE INDEX IF NOT EXISTS idx_ne_source_deposit ON tax.normalized_events(source_deposit_id);
CREATE INDEX IF NOT EXISTS idx_ne_paired ON tax.normalized_events(paired_event_id);

-- ═══════════════════════════════════════════════════════════════════════════
-- VALUATION LOG
-- Every price lookup is recorded with full provenance.
-- Reviewer Issue 5: audit-grade valuation with actual source tracking.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.valuation_log (
    id                  SERIAL PRIMARY KEY,
    asset               VARCHAR(50) NOT NULL,
    event_at            TIMESTAMPTZ NOT NULL,   -- exact event time (not just date)
    price_date          DATE NOT NULL,          -- date used for lookup
    price_usd           NUMERIC(36,18),
    source_name         VARCHAR(50) NOT NULL,   -- 'coingecko', 'nonkyc', 'manual', 'stablecoin_peg'
    source_id           VARCHAR(200),           -- CoinGecko coin ID, etc.
    source_timestamp    TIMESTAMPTZ,            -- when the price was quoted
    granularity         VARCHAR(20),            -- 'daily', 'hourly', 'minute', 'tick'
    is_estimated        BOOLEAN DEFAULT FALSE,  -- true if not a direct historical lookup
    is_manual           BOOLEAN DEFAULT FALSE,
    fallback_reason     TEXT,                   -- why a fallback source was used
    retrieval_at        TIMESTAMPTZ DEFAULT NOW(),
    run_id              INTEGER
);
CREATE INDEX IF NOT EXISTS idx_vl_asset ON tax.valuation_log(asset, event_at);

-- ═══════════════════════════════════════════════════════════════════════════
-- WALLET-AWARE LOTS (v4)
-- Reviewer Issues 2+3: per-wallet FIFO with parent lot tracking for
-- transfers. Original acquisition date preserved through transfers.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.lots_v4 (
    id                  SERIAL PRIMARY KEY,
    asset               VARCHAR(50) NOT NULL,
    asset_chain         VARCHAR(50),
    asset_contract      VARCHAR(200),

    -- Wallet/account (required for per-wallet FIFO)
    wallet              VARCHAR(100) NOT NULL,

    -- Quantities
    original_quantity   NUMERIC(36,18) NOT NULL,
    remaining           NUMERIC(36,18) NOT NULL,

    -- Cost basis
    cost_per_unit_usd   NUMERIC(36,18),
    total_cost_usd      NUMERIC(36,18),

    -- Dates: original_acquired_at NEVER changes, even through transfers
    original_acquired_at TIMESTAMPTZ NOT NULL,
    lot_created_at       TIMESTAMPTZ NOT NULL,  -- when this lot record was created

    -- Source
    source_event_id     INTEGER,                -- FK to tax.normalized_events
    source_type         VARCHAR(30),            -- 'trade', 'income', 'transfer_in', 'deposit'

    -- Transfer lineage
    parent_lot_id       INTEGER,                -- FK to tax.lots_v4 (the lot this was split from)
    transfer_carryover_id INTEGER,              -- FK to tax.transfer_carryover

    -- Audit
    is_depleted         BOOLEAN DEFAULT FALSE,
    manual_notes        TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    run_id              INTEGER
);
CREATE INDEX IF NOT EXISTS idx_lots4_wallet ON tax.lots_v4(wallet, asset, original_acquired_at);
CREATE INDEX IF NOT EXISTS idx_lots4_remaining ON tax.lots_v4(asset, wallet) WHERE remaining > 0;
CREATE INDEX IF NOT EXISTS idx_lots4_parent ON tax.lots_v4(parent_lot_id);

-- ═══════════════════════════════════════════════════════════════════════════
-- TRANSFER CARRYOVER
-- Reviewer Issue 2: lot-slice relocation with full provenance.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.transfer_carryover (
    id                  SERIAL PRIMARY KEY,
    asset               VARCHAR(50) NOT NULL,
    quantity            NUMERIC(36,18) NOT NULL,

    -- Source
    source_wallet       VARCHAR(100) NOT NULL,
    source_lot_id       INTEGER NOT NULL,       -- FK to tax.lots_v4
    source_event_id     INTEGER,                -- the TRANSFER_OUT normalized event

    -- Destination
    dest_wallet         VARCHAR(100) NOT NULL,
    dest_lot_id         INTEGER,                -- FK to tax.lots_v4 (the continuation lot)
    dest_event_id       INTEGER,                -- the TRANSFER_IN normalized event

    -- Preserved from original lot
    original_acquired_at TIMESTAMPTZ NOT NULL,
    carryover_basis_usd  NUMERIC(36,18),
    cost_per_unit_usd    NUMERIC(36,18),

    -- Transfer metadata
    transferred_at      TIMESTAMPTZ NOT NULL,
    tx_hash             VARCHAR(500),
    transfer_fee        NUMERIC(36,18),
    transfer_fee_asset  VARCHAR(50),
    transfer_fee_usd    NUMERIC(36,18),

    -- Matching evidence
    withdrawal_id       INTEGER,
    deposit_id          INTEGER,
    match_confidence    VARCHAR(20),            -- 'tx_hash', 'amount_timing', 'manual'

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    run_id              INTEGER
);
CREATE INDEX IF NOT EXISTS idx_tc_source ON tax.transfer_carryover(source_wallet, asset);
CREATE INDEX IF NOT EXISTS idx_tc_dest ON tax.transfer_carryover(dest_wallet, asset);

-- ═══════════════════════════════════════════════════════════════════════════
-- DISPOSALS (v4)
-- Full provenance: which lot was consumed, original acquisition date,
-- proper holding period (>1 year for long-term per IRS rule).
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.disposals_v4 (
    id                  SERIAL PRIMARY KEY,
    asset               VARCHAR(50) NOT NULL,
    wallet              VARCHAR(100) NOT NULL,
    quantity            NUMERIC(36,18) NOT NULL,

    -- Proceeds
    proceeds_usd        NUMERIC(36,18),
    fee_usd             NUMERIC(36,18),
    net_proceeds_usd    NUMERIC(36,18),

    -- Cost basis (from consumed lot)
    cost_basis_usd      NUMERIC(36,18),
    gain_loss_usd       NUMERIC(36,18),

    -- Dates
    original_acquired_at TIMESTAMPTZ,          -- from the lot, preserved through transfers
    disposed_at          TIMESTAMPTZ NOT NULL,
    holding_days         INTEGER,
    -- IRS rule: long-term = held MORE THAN one year (not >= 365 days)
    term                 VARCHAR(10),           -- 'short' or 'long'

    -- Provenance
    disposal_event_id    INTEGER,               -- FK to normalized_events (DISPOSAL)
    lot_id               INTEGER,               -- FK to lots_v4
    source_trade_id      INTEGER,

    -- Context
    market              VARCHAR(100),
    exchange            VARCHAR(50),

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    run_id              INTEGER
);
CREATE INDEX IF NOT EXISTS idx_disp4_date ON tax.disposals_v4(disposed_at);
CREATE INDEX IF NOT EXISTS idx_disp4_wallet ON tax.disposals_v4(wallet, asset);

-- ═══════════════════════════════════════════════════════════════════════════
-- INCOME EVENTS (v4)
-- Reviewer Issue 4: evidence-based, not heuristic. Manual review status.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.income_events_v4 (
    id                  SERIAL PRIMARY KEY,
    wallet              VARCHAR(100) NOT NULL,
    asset               VARCHAR(50) NOT NULL,
    quantity            NUMERIC(36,18) NOT NULL,
    fmv_per_unit_usd    NUMERIC(36,18),
    total_fmv_usd       NUMERIC(36,18),

    -- Classification
    income_type         VARCHAR(50) NOT NULL,
    -- Valid types: 'staking', 'mining', 'airdrop', 'hard_fork', 'interest',
    --   'referral', 'promotional', 'compensation', 'pool_reward', 'other'
    classification_evidence TEXT,               -- why this classification was chosen
    classification_source VARCHAR(50),          -- 'exchange_api', 'manual', 'heuristic'

    -- Review status
    review_status       VARCHAR(20) DEFAULT 'pending',
    -- 'pending', 'confirmed', 'rejected', 'needs_review'
    reviewer_notes      TEXT,

    -- Dominion/control timestamp (when taxpayer gained control)
    dominion_at         TIMESTAMPTZ NOT NULL,

    -- Valuation
    valuation_id        INTEGER,               -- FK to tax.valuation_log
    valuation_source    VARCHAR(50),
    valuation_timestamp TIMESTAMPTZ,

    -- Source linkage
    source_event_id     INTEGER,               -- FK to normalized_events
    source_deposit_id   INTEGER,
    source_pool_id      INTEGER,

    -- Lot created from this income
    lot_id              INTEGER,               -- FK to lots_v4

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    run_id              INTEGER
);
CREATE INDEX IF NOT EXISTS idx_inc4_type ON tax.income_events_v4(income_type, dominion_at);
CREATE INDEX IF NOT EXISTS idx_inc4_status ON tax.income_events_v4(review_status);

-- ═══════════════════════════════════════════════════════════════════════════
-- EXCEPTION SYSTEM
-- Reviewer Issue 6: hard-stop for unsupported types, blocking exceptions
-- prevent filing, all issues tracked with severity and dollar exposure.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.exceptions (
    id                  SERIAL PRIMARY KEY,
    severity            VARCHAR(10) NOT NULL,   -- 'BLOCKING', 'WARNING', 'INFO'
    category            VARCHAR(50) NOT NULL,
    -- Categories:
    --   'UNKNOWN_BASIS'         — disposal with no matching lot
    --   'MISSING_PRICE'         — no historical FMV available
    --   'UNMATCHED_TRANSFER'    — withdrawal/deposit not paired
    --   'UNSUPPORTED_TX_TYPE'   — pool swap, bridge, futures, etc.
    --   'AMBIGUOUS_DEPOSIT'     — deposit not classifiable
    --   'DUPLICATE_SUSPICION'   — possible duplicate import
    --   'OVERSOLD'              — negative inventory condition
    --   'TIMESTAMP_INVALID'     — missing or suspicious timestamp
    --   'STABLECOIN_DEPEG'      — stablecoin used at $1.00 during depeg
    --   'CRYPTO_TO_CRYPTO'      — c2c trade (BLOCKING if engine can't handle)
    --   'HOLDING_PERIOD_RESET'  — transfer may have reset holding period
    --   'VALUATION_FALLBACK'    — used non-historical price source

    message             TEXT NOT NULL,
    detail              TEXT,

    -- Affected records
    source_trade_id     INTEGER,
    source_deposit_id   INTEGER,
    source_withdrawal_id INTEGER,
    source_event_id     INTEGER,
    lot_id              INTEGER,

    -- Impact
    dollar_exposure     NUMERIC(36,2),         -- estimated USD impact
    affected_tax_year   INTEGER,

    -- Resolution
    resolution_status   VARCHAR(20) DEFAULT 'open',
    -- 'open', 'resolved', 'accepted_risk', 'manual_override'
    resolution_notes    TEXT,
    resolved_by         VARCHAR(100),
    resolved_at         TIMESTAMPTZ,

    -- Filing gate
    blocks_filing       BOOLEAN DEFAULT FALSE,

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    run_id              INTEGER
);
CREATE INDEX IF NOT EXISTS idx_exc_severity ON tax.exceptions(severity, category);
CREATE INDEX IF NOT EXISTS idx_exc_filing ON tax.exceptions(blocks_filing) WHERE blocks_filing = TRUE;
CREATE INDEX IF NOT EXISTS idx_exc_status ON tax.exceptions(resolution_status);

-- ═══════════════════════════════════════════════════════════════════════════
-- RUN MANIFEST
-- Reproducibility: every computation run is recorded with metadata.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.run_manifest (
    id                  SERIAL PRIMARY KEY,
    run_type            VARCHAR(30) NOT NULL,   -- 'full', 'incremental', 'recompute'
    tax_year            INTEGER,
    basis_method        VARCHAR(20) NOT NULL,   -- 'FIFO', 'LIFO', 'HIFO', 'SPEC_ID'
    wallet_aware        BOOLEAN DEFAULT TRUE,
    code_version        VARCHAR(100),
    config_snapshot     JSONB,
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    status              VARCHAR(20) DEFAULT 'running',
    -- 'running', 'completed', 'failed', 'filing_blocked'
    total_events        INTEGER,
    total_disposals     INTEGER,
    total_exceptions    INTEGER,
    blocking_exceptions INTEGER,
    filing_ready        BOOLEAN DEFAULT FALSE,
    error_message       TEXT
);

-- ═══════════════════════════════════════════════════════════════════════════
-- FORM 8949 (v4)
-- Year-aware box logic, linked to run manifest.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.form_8949_v4 (
    id                  SERIAL PRIMARY KEY,
    description         TEXT,
    date_acquired       VARCHAR(20),
    date_sold           VARCHAR(20),
    proceeds            NUMERIC(36,2),
    cost_basis          NUMERIC(36,2),
    adjustment_code     VARCHAR(10),
    adjustment_amount   NUMERIC(36,2),
    gain_loss           NUMERIC(36,2),
    term                VARCHAR(10),
    box                 VARCHAR(5),
    asset               VARCHAR(50),
    wallet              VARCHAR(100),
    exchange            VARCHAR(50),
    holding_days        INTEGER,
    is_futures          BOOLEAN DEFAULT FALSE,
    tax_year            INTEGER NOT NULL,
    disposal_id         INTEGER,               -- FK to disposals_v4
    run_id              INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_f8949v4_year ON tax.form_8949_v4(tax_year);
CREATE INDEX IF NOT EXISTS idx_f8949v4_run ON tax.form_8949_v4(run_id);

-- ═══════════════════════════════════════════════════════════════════════════
-- SUPPORTED TRANSACTION TYPES
-- Reviewer Issue 6: explicit taxonomy of what's supported/unsupported.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.tx_type_support (
    id                  SERIAL PRIMARY KEY,
    tx_type             VARCHAR(50) NOT NULL UNIQUE,
    is_supported        BOOLEAN NOT NULL,
    blocks_filing       BOOLEAN DEFAULT TRUE,  -- if unsupported, does it block?
    notes               TEXT,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Seed the taxonomy
INSERT INTO tax.tx_type_support (tx_type, is_supported, blocks_filing, notes) VALUES
    ('spot_buy_fiat', TRUE, FALSE, 'Buy crypto with fiat — fully supported'),
    ('spot_sell_fiat', TRUE, FALSE, 'Sell crypto for fiat — fully supported'),
    ('spot_crypto_to_crypto', TRUE, FALSE, 'Crypto-to-crypto trade — double-entry decomposition'),
    ('stablecoin_trade', TRUE, FALSE, 'Stablecoin pair trade — treated as crypto-to-crypto'),
    ('transfer_self', TRUE, FALSE, 'Self-transfer between wallets — lot relocation'),
    ('staking_reward', TRUE, FALSE, 'Staking income — ordinary income at FMV'),
    ('pool_reward', TRUE, FALSE, 'Pool/LP reward — ordinary income at FMV'),
    ('airdrop', TRUE, FALSE, 'Airdrop receipt — ordinary income when dominion gained'),
    ('fee_crypto', TRUE, FALSE, 'Fee paid in crypto — separate disposal event'),
    ('deposit_unclassified', FALSE, TRUE, 'Deposit not yet classified — BLOCKS filing'),
    ('futures_perpetual', FALSE, TRUE, 'Futures/perps — NOT YET SUPPORTED'),
    ('options', FALSE, TRUE, 'Options — NOT YET SUPPORTED'),
    ('margin_trade', FALSE, TRUE, 'Margin trading — NOT YET SUPPORTED'),
    ('liquidation', FALSE, TRUE, 'Liquidation — NOT YET SUPPORTED'),
    ('bridge', FALSE, TRUE, 'Bridge between chains — NOT YET SUPPORTED'),
    ('wrap_unwrap', FALSE, TRUE, 'Wrapped token conversion — NOT YET SUPPORTED'),
    ('nft_trade', FALSE, TRUE, 'NFT buy/sell — NOT YET SUPPORTED'),
    ('nft_mint', FALSE, TRUE, 'NFT minting — NOT YET SUPPORTED'),
    ('lending_deposit', FALSE, TRUE, 'Lending deposit — NOT YET SUPPORTED'),
    ('lending_withdrawal', FALSE, TRUE, 'Lending withdrawal — NOT YET SUPPORTED'),
    ('interest_income', FALSE, FALSE, 'Interest/lending income — manual classification needed'),
    ('hard_fork', FALSE, TRUE, 'Hard fork receipt — NOT YET SUPPORTED'),
    ('mining', FALSE, FALSE, 'Mining income — manual classification needed'),
    ('compensation', FALSE, FALSE, 'Crypto compensation — manual classification needed'),
    ('lp_add', FALSE, TRUE, 'Liquidity pool add — NOT YET SUPPORTED'),
    ('lp_remove', FALSE, TRUE, 'Liquidity pool remove — NOT YET SUPPORTED'),
    ('pool_swap', FALSE, TRUE, 'Pool swap — NOT YET SUPPORTED'),
    ('referral_bonus', FALSE, FALSE, 'Referral bonus — manual classification needed')
ON CONFLICT (tx_type) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════
-- DATA COVERAGE / SOURCE TRACKING
-- Records what date ranges each exchange's API actually covers,
-- and whether CSV imports were used to fill gaps.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.data_coverage (
    id                  SERIAL PRIMARY KEY,
    exchange            VARCHAR(50) NOT NULL,
    data_type           VARCHAR(50) NOT NULL,
    api_earliest        TIMESTAMPTZ,
    api_latest          TIMESTAMPTZ,
    csv_earliest        TIMESTAMPTZ,
    csv_latest          TIMESTAMPTZ,
    has_gap             BOOLEAN DEFAULT FALSE,
    gap_description     TEXT,
    requires_csv        BOOLEAN DEFAULT FALSE,
    csv_imported        BOOLEAN DEFAULT FALSE,
    run_id              INTEGER,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dc_exchange ON tax.data_coverage(exchange, data_type);

-- ═══════════════════════════════════════════════════════════════════════════
-- CSV IMPORT TRACKING
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.csv_imports (
    id                  SERIAL PRIMARY KEY,
    exchange            VARCHAR(50) NOT NULL,
    data_type           VARCHAR(50) NOT NULL,
    filename            VARCHAR(500) NOT NULL,
    file_hash           VARCHAR(64) NOT NULL,
    row_count           INTEGER NOT NULL,
    imported_count      INTEGER NOT NULL,
    duplicate_count     INTEGER NOT NULL,
    error_count         INTEGER NOT NULL,
    date_range_start    TIMESTAMPTZ,
    date_range_end      TIMESTAMPTZ,
    imported_at         TIMESTAMPTZ DEFAULT NOW(),
    imported_by         VARCHAR(100) DEFAULT 'system'
);

-- ═══════════════════════════════════════════════════════════════════════════
-- CLASSIFIED FUNDING FLOWS
-- Every deposit and withdrawal is classified for accountant reporting.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.classified_flows (
    id                  SERIAL PRIMARY KEY,
    source_type         VARCHAR(20) NOT NULL,
    source_id           INTEGER NOT NULL,
    exchange            VARCHAR(50) NOT NULL,
    asset               VARCHAR(50) NOT NULL,
    quantity            NUMERIC(36,18) NOT NULL,
    unit_price_usd      NUMERIC(36,18),
    total_usd            NUMERIC(36,18),
    flow_class          VARCHAR(30) NOT NULL,
    classification_rule VARCHAR(200),
    manual_override     BOOLEAN DEFAULT FALSE,
    manual_notes        TEXT,
    event_at            TIMESTAMPTZ NOT NULL,
    run_id              INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cf_exchange ON tax.classified_flows(exchange, flow_class);
CREATE INDEX IF NOT EXISTS idx_cf_class ON tax.classified_flows(flow_class);

-- Run-scoped indexes for v4 tables
CREATE INDEX IF NOT EXISTS idx_normalized_events_run_id ON tax.normalized_events(run_id, event_at, id);
CREATE INDEX IF NOT EXISTS idx_lots_v4_run_id ON tax.lots_v4(run_id, wallet, asset);
CREATE INDEX IF NOT EXISTS idx_disposals_v4_run_id ON tax.disposals_v4(run_id, disposed_at, id);
CREATE INDEX IF NOT EXISTS idx_transfer_carryover_run_id ON tax.transfer_carryover(run_id);
CREATE INDEX IF NOT EXISTS idx_classified_flows_run_id ON tax.classified_flows(run_id, exchange, flow_class);
CREATE INDEX IF NOT EXISTS idx_income_events_v4_run_id ON tax.income_events_v4(run_id);
CREATE INDEX IF NOT EXISTS idx_exceptions_run_id ON tax.exceptions(run_id, severity, category);

-- ═══════════════════════════════════════════════════════════════════════════
-- EXCHANGE INTERNAL TRANSFERS (e.g. MEXC universal transfers spot→futures)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.exchange_transfers (
    id              SERIAL PRIMARY KEY,
    exchange        VARCHAR(50)   NOT NULL,
    exchange_id     VARCHAR(200)  NOT NULL,
    asset           VARCHAR(50)   NOT NULL,
    amount          NUMERIC(36,18) NOT NULL,
    from_account    VARCHAR(100),
    to_account      VARCHAR(100),
    status          VARCHAR(30),
    asset_price_usd NUMERIC(36,18),
    amount_usd      NUMERIC(36,18),
    transferred_at  TIMESTAMPTZ,
    raw_data        JSONB,
    source_type     VARCHAR(30) DEFAULT 'api',
    source_file     VARCHAR(500),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(exchange, exchange_id)
);
CREATE INDEX IF NOT EXISTS idx_ext_exchange ON tax.exchange_transfers(exchange, transferred_at);

-- ═══════════════════════════════════════════════════════════════════════════
-- WALLET OWNERSHIP / ADDRESS CLAIMS
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tax.wallet_entities (
    id              SERIAL PRIMARY KEY,
    entity_type     VARCHAR(30) NOT NULL,
    label           VARCHAR(200) NOT NULL,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tax.wallet_accounts (
    id              SERIAL PRIMARY KEY,
    entity_id       INTEGER NOT NULL REFERENCES tax.wallet_entities(id),
    account_type    VARCHAR(30) NOT NULL,
    exchange_name   VARCHAR(50),
    label           VARCHAR(200) NOT NULL,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tax.wallet_addresses (
    id              SERIAL PRIMARY KEY,
    account_id      INTEGER NOT NULL REFERENCES tax.wallet_accounts(id),
    address         VARCHAR(500) NOT NULL,
    chain           VARCHAR(50),
    network         VARCHAR(100),
    token_contract  VARCHAR(200),
    label           VARCHAR(200),
    first_seen_at   TIMESTAMPTZ,
    last_seen_at    TIMESTAMPTZ,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(address, chain)
);
CREATE INDEX IF NOT EXISTS idx_wa_address ON tax.wallet_addresses(address);
CREATE INDEX IF NOT EXISTS idx_wa_account ON tax.wallet_addresses(account_id);

CREATE TABLE IF NOT EXISTS tax.wallet_address_claims (
    id              SERIAL PRIMARY KEY,
    address_id      INTEGER NOT NULL REFERENCES tax.wallet_addresses(id),
    claim_type      VARCHAR(30) NOT NULL,
    confidence      VARCHAR(20) NOT NULL,
    effective_from  TIMESTAMPTZ,
    effective_to    TIMESTAMPTZ,
    evidence_summary TEXT,
    claimed_by      VARCHAR(100) DEFAULT 'user',
    claimed_at      TIMESTAMPTZ DEFAULT NOW(),
    reviewed_by     VARCHAR(100),
    reviewed_at     TIMESTAMPTZ,
    review_status   VARCHAR(20) DEFAULT 'pending',
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tax.import_stages (
    id TEXT PRIMARY KEY,
    exchange TEXT NOT NULL,
    data_type TEXT NOT NULL,
    filename TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'staged',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    committed_at TIMESTAMPTZ,
    expired_at TIMESTAMPTZ,
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS tax.import_stage_rows (
    id SERIAL PRIMARY KEY,
    stage_id TEXT NOT NULL REFERENCES tax.import_stages(id) ON DELETE CASCADE,
    row_index INTEGER NOT NULL,
    parsed JSONB NOT NULL,
    raw JSONB,
    status TEXT NOT NULL DEFAULT 'new',
    match_info JSONB,
    decision TEXT,
    result TEXT,
    result_id INTEGER,
    error TEXT,
    UNIQUE(stage_id, row_index)
);

CREATE TABLE IF NOT EXISTS tax.asset_aliases (
    id              SERIAL PRIMARY KEY,
    exchange        VARCHAR(50),
    raw_asset       VARCHAR(100) NOT NULL,
    network         VARCHAR(100),
    canonical_asset VARCHAR(50) NOT NULL,
    confidence      VARCHAR(20) DEFAULT 'verified',
    notes           TEXT,
    UNIQUE(exchange, raw_asset, network)
);

CREATE TABLE IF NOT EXISTS tax.activity_start (
    exchange TEXT PRIMARY KEY,
    start_date DATE NOT NULL,
    notes TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tax.wallet_claim_evidence (
    id              SERIAL PRIMARY KEY,
    claim_id        INTEGER NOT NULL REFERENCES tax.wallet_address_claims(id),
    evidence_type   VARCHAR(50) NOT NULL,
    evidence_data   JSONB,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
"""
