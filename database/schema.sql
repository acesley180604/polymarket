-- PolySpy Database Schema
-- "Audited Intelligence Platform" for Polymarket Whale Tracking

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- WALLETS TABLE
-- Stores whale profiles with credit ratings and persona tags
-- ============================================
CREATE TABLE wallets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    address VARCHAR(42) UNIQUE NOT NULL,  -- Ethereum address (0x + 40 hex chars)
    alias VARCHAR(100),                    -- Human-readable alias if known

    -- Credit Rating System (The "Audited Whale" Strategy)
    credit_grade VARCHAR(3) NOT NULL DEFAULT 'C',  -- AAA, AA, A, B, C, F
    credit_score INTEGER DEFAULT 0 CHECK (credit_score >= 0 AND credit_score <= 100),

    -- Persona Auto-Tagging (The "Narrative" Strategy)
    persona_tag VARCHAR(50),  -- 'The Insider', 'The Contrarian', 'The Whale Trap', etc.

    -- Performance Metrics
    win_rate DECIMAL(5,2) DEFAULT 0.00,           -- % of profitable trades (0-100)
    pnl_ratio DECIMAL(8,2) DEFAULT 0.00,          -- Avg Win / Avg Loss ratio
    consistency_score DECIMAL(5,2) DEFAULT 0.00,  -- Lower std dev = higher score
    avg_holding_hours DECIMAL(10,2) DEFAULT 0.00, -- Average position duration

    -- Aggregate Stats
    total_trades INTEGER DEFAULT 0,
    total_wins INTEGER DEFAULT 0,
    total_losses INTEGER DEFAULT 0,
    total_profit_usd DECIMAL(18,2) DEFAULT 0.00,
    total_loss_usd DECIMAL(18,2) DEFAULT 0.00,
    total_volume_usd DECIMAL(18,2) DEFAULT 0.00,

    -- Timestamps
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_active_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for quick lookups
CREATE INDEX idx_wallets_address ON wallets(address);
CREATE INDEX idx_wallets_credit_grade ON wallets(credit_grade);
CREATE INDEX idx_wallets_persona_tag ON wallets(persona_tag);
CREATE INDEX idx_wallets_total_volume ON wallets(total_volume_usd DESC);

-- ============================================
-- TRANSACTIONS TABLE
-- Stores all whale trades with dump alert flags
-- ============================================
CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    wallet_address VARCHAR(42) NOT NULL REFERENCES wallets(address),

    -- Market Info
    market_slug VARCHAR(255) NOT NULL,      -- Polymarket market identifier
    market_question TEXT,                    -- Human-readable market question
    outcome VARCHAR(10) NOT NULL,           -- 'YES' or 'NO'

    -- Transaction Type (The "Exit Liquidity Savior" Strategy)
    type VARCHAR(10) NOT NULL CHECK (type IN ('BUY', 'SELL', 'EXIT', 'REDEEM')),

    -- Trade Details
    amount_usd DECIMAL(18,2) NOT NULL,      -- Dollar value of trade
    shares DECIMAL(18,6) NOT NULL,          -- Number of shares
    price DECIMAL(8,6) NOT NULL,            -- Price per share (0.00 - 1.00)

    -- Contextual EV Data (The "Copy-Trading" Strategy)
    entry_price DECIMAL(8,6),               -- Original entry price for PnL calc
    potential_roi DECIMAL(8,2),             -- ((1 - price) / price) * 100 at time of signal

    -- Alert Flags
    is_dump_alert BOOLEAN DEFAULT FALSE,    -- TRUE if whale is exiting position
    is_insider_signal BOOLEAN DEFAULT FALSE, -- TRUE if matches "Insider" pattern

    -- Resolution Data (filled after market resolves)
    is_profitable BOOLEAN,                  -- Was this trade profitable?
    realized_pnl DECIMAL(18,2),             -- Actual profit/loss in USD

    -- Blockchain Data
    tx_hash VARCHAR(66),                    -- Transaction hash
    block_number BIGINT,

    -- Timestamps
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_transactions_wallet ON transactions(wallet_address);
CREATE INDEX idx_transactions_market ON transactions(market_slug);
CREATE INDEX idx_transactions_type ON transactions(type);
CREATE INDEX idx_transactions_dump_alert ON transactions(is_dump_alert) WHERE is_dump_alert = TRUE;
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp DESC);

-- ============================================
-- MARKETS TABLE
-- Stores Polymarket market metadata
-- ============================================
CREATE TABLE markets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    slug VARCHAR(255) UNIQUE NOT NULL,
    question TEXT NOT NULL,
    description TEXT,

    -- Current State
    current_yes_price DECIMAL(8,6),
    current_no_price DECIMAL(8,6),
    volume_usd DECIMAL(18,2) DEFAULT 0.00,
    liquidity_usd DECIMAL(18,2) DEFAULT 0.00,

    -- Resolution
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_outcome VARCHAR(10),  -- 'YES', 'NO', or NULL
    resolved_at TIMESTAMPTZ,

    -- Timestamps
    end_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_markets_slug ON markets(slug);
CREATE INDEX idx_markets_resolved ON markets(is_resolved);

-- ============================================
-- SIGNALS TABLE
-- Stores generated trading signals for the dashboard
-- ============================================
CREATE TABLE signals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID REFERENCES transactions(id),
    wallet_address VARCHAR(42) NOT NULL REFERENCES wallets(address),
    market_slug VARCHAR(255) NOT NULL,

    -- Signal Metadata
    signal_type VARCHAR(20) NOT NULL,  -- 'WHALE_BUY', 'WHALE_SELL', 'DUMP_ALERT', 'INSIDER_MOVE'

    -- Whale Info (denormalized for fast reads)
    whale_grade VARCHAR(3) NOT NULL,
    whale_persona VARCHAR(50),
    whale_win_rate DECIMAL(5,2),

    -- Trade Info
    outcome VARCHAR(10) NOT NULL,
    amount_usd DECIMAL(18,2) NOT NULL,
    price DECIMAL(8,6) NOT NULL,

    -- EV Calculation (The "Contextual Copy-Trading" Strategy)
    copy_ev_percent DECIMAL(8,2),  -- ((1 - price) / price) * 100

    -- UI State
    is_read BOOLEAN DEFAULT FALSE,
    is_dismissed BOOLEAN DEFAULT FALSE,

    -- Timestamps
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_signals_type ON signals(signal_type);
CREATE INDEX idx_signals_timestamp ON signals(timestamp DESC);
CREATE INDEX idx_signals_unread ON signals(is_read) WHERE is_read = FALSE;

-- ============================================
-- VIEWS for Dashboard Queries
-- ============================================

-- Top Whales View (AAA and A grade only)
CREATE VIEW top_whales AS
SELECT
    address,
    alias,
    credit_grade,
    credit_score,
    persona_tag,
    win_rate,
    pnl_ratio,
    total_profit_usd,
    total_volume_usd,
    last_active_at
FROM wallets
WHERE credit_grade IN ('AAA', 'AA', 'A')
ORDER BY credit_score DESC, total_profit_usd DESC;

-- Recent Dump Alerts View
CREATE VIEW dump_alerts AS
SELECT
    t.id,
    t.wallet_address,
    w.alias,
    w.credit_grade,
    w.persona_tag,
    t.market_slug,
    t.market_question,
    t.outcome,
    t.amount_usd,
    t.price,
    t.timestamp
FROM transactions t
JOIN wallets w ON t.wallet_address = w.address
WHERE t.is_dump_alert = TRUE
ORDER BY t.timestamp DESC;

-- ============================================
-- FUNCTIONS & TRIGGERS
-- ============================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_wallets_updated_at
    BEFORE UPDATE ON wallets
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_markets_updated_at
    BEFORE UPDATE ON markets
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
