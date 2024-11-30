-- Grant privileges on the public schema to the current user
GRANT ALL PRIVILEGES ON SCHEMA public TO CURRENT_USER;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create enum type for event
CREATE TYPE tx_event_type AS ENUM ('coinbase', 'transfer');

-- Create the base table for transactions
CREATE TABLE IF NOT EXISTS transactions (
    tx_id VARCHAR(64) NOT NULL,
    tx_index INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    block_height INTEGER NOT NULL,
    is_coinbase BOOLEAN NOT NULL,
    in_total_amount NUMERIC(20, 0) NOT NULL,
    out_total_amount NUMERIC(20, 0) NOT NULL,
    fee_amount NUMERIC(20, 0) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tx_id, timestamp),
    CONSTRAINT check_fee CHECK (
        (NOT is_coinbase AND fee_amount = in_total_amount - out_total_amount) OR
        (is_coinbase AND fee_amount = 0)
    )
);

-- Create hypertable with 1 day intervals for time-series optimization
SELECT create_hypertable('transactions', 'timestamp',
    chunk_time_interval => INTERVAL '1 day');

-- Create table for inputs (vins)
CREATE TABLE IF NOT EXISTS transaction_inputs (
    id BIGSERIAL PRIMARY KEY,
    tx_id VARCHAR(64) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    address VARCHAR(64) NOT NULL,
    amount NUMERIC(20, 0) NOT NULL
);

-- Create table for outputs (vouts)
CREATE TABLE IF NOT EXISTS transaction_outputs (
    id BIGSERIAL PRIMARY KEY,
    tx_id VARCHAR(64) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    address VARCHAR(64) NOT NULL,
    amount NUMERIC(20, 0) NOT NULL
);

-- Create indexes for transaction lookups
CREATE INDEX IF NOT EXISTS idx_inputs_tx_timestamp ON transaction_inputs (tx_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_outputs_tx_timestamp ON transaction_outputs (tx_id, timestamp);

-- Create optimized indexes
CREATE INDEX IF NOT EXISTS idx_tx_block_height ON transactions (block_height);
CREATE INDEX IF NOT EXISTS idx_tx_coinbase ON transactions (is_coinbase);
CREATE INDEX IF NOT EXISTS idx_inputs_address ON transaction_inputs (address);
CREATE INDEX IF NOT EXISTS idx_inputs_timestamp ON transaction_inputs (timestamp);
CREATE INDEX IF NOT EXISTS idx_outputs_address ON transaction_outputs (address);
CREATE INDEX IF NOT EXISTS idx_outputs_timestamp ON transaction_outputs (timestamp);

-- Balance changes table
CREATE TABLE IF NOT EXISTS balance_changes (
    address VARCHAR NOT NULL,
    block_height INTEGER NOT NULL,
    event tx_event_type NOT NULL,
    balance_delta NUMERIC(20, 8) NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (address, block_height)
);

-- Create indexes for balance_changes
CREATE INDEX IF NOT EXISTS idx_balance_changes_block_height ON balance_changes(block_height);
CREATE INDEX IF NOT EXISTS idx_balance_changes_addr_time ON balance_changes(address, block_timestamp DESC);

-- Blocks table
CREATE TABLE IF NOT EXISTS blocks (
    block_height INTEGER PRIMARY KEY,
    timestamp TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_timestamp ON blocks(timestamp);

-- Balance address blocks table
CREATE TABLE IF NOT EXISTS balance_address_blocks (
    address VARCHAR NOT NULL,
    block_height INTEGER NOT NULL,
    balance NUMERIC(20, 8) NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (address, block_height)
);

-- Create materialized view for latest balances
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_address_balances AS
SELECT DISTINCT ON (address)
    address,
    block_height,
    balance
FROM balance_address_blocks
ORDER BY address, block_height DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_latest_balances ON latest_address_balances(address);

-- Optimized indexes for balance_address_blocks
CREATE INDEX IF NOT EXISTS idx_bab_addr_height_lookup
    ON balance_address_blocks (address, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_bab_height_balance_top
    ON balance_address_blocks (block_height DESC, balance DESC)
    INCLUDE (address);
CREATE INDEX IF NOT EXISTS idx_bab_height_balance_range
    ON balance_address_blocks (block_height DESC, balance)
    INCLUDE (address);
CREATE INDEX IF NOT EXISTS idx_bab_timestamp_balance
    ON balance_address_blocks (block_timestamp DESC, balance)
    INCLUDE (address, block_height);
CREATE INDEX IF NOT EXISTS idx_bab_addr_time
    ON balance_address_blocks (address, block_timestamp DESC)
    INCLUDE (balance, block_height);

-- Large balances index (10 BTC or more)
CREATE INDEX IF NOT EXISTS idx_large_balances
    ON balance_address_blocks (address, balance)
    WHERE balance >= 10;

-- Conversion function
CREATE OR REPLACE FUNCTION satoshi_to_btc(satoshi_amount NUMERIC)
RETURNS NUMERIC AS $$
BEGIN
    RETURN (satoshi_amount::NUMERIC / 100000000);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Optimized trigger function for balance changes
CREATE OR REPLACE FUNCTION process_transaction_balance_changes()
RETURNS TRIGGER AS $$
DECLARE
    current_block_height INTEGER;
BEGIN
    -- Store block height to avoid repeated lookups
    current_block_height := NEW.block_height;

    IF TG_OP = 'INSERT' THEN
        IF NEW.is_coinbase THEN
            -- Handle coinbase transaction
            INSERT INTO balance_changes (
                address,
                block_height,
                event,
                balance_delta,
                block_timestamp
            )
            SELECT
                address,
                current_block_height,
                'coinbase'::tx_event_type,
                satoshi_to_btc(amount),
                NEW.timestamp
            FROM transaction_outputs
            WHERE tx_id = NEW.tx_id
            ON CONFLICT (address, block_height) DO UPDATE
            SET balance_delta = balance_changes.balance_delta + EXCLUDED.balance_delta;
        ELSE
            -- Handle inputs (negative balance changes)
            INSERT INTO balance_changes (
                address,
                block_height,
                event,
                balance_delta,
                block_timestamp
            )
            SELECT
                address,
                current_block_height,
                'transfer'::tx_event_type,
                satoshi_to_btc(-amount),
                NEW.timestamp
            FROM transaction_inputs
            WHERE tx_id = NEW.tx_id
            ON CONFLICT (address, block_height) DO UPDATE
            SET balance_delta = balance_changes.balance_delta + EXCLUDED.balance_delta;

            -- Handle outputs (positive balance changes)
            INSERT INTO balance_changes (
                address,
                block_height,
                event,
                balance_delta,
                block_timestamp
            )
            SELECT
                address,
                current_block_height,
                'transfer'::tx_event_type,
                satoshi_to_btc(amount),
                NEW.timestamp
            FROM transaction_outputs
            WHERE tx_id = NEW.tx_id
            ON CONFLICT (address, block_height) DO UPDATE
            SET balance_delta = balance_changes.balance_delta + EXCLUDED.balance_delta;
        END IF;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Optimized trigger function for block data
CREATE OR REPLACE FUNCTION process_block_data()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO blocks (block_height, timestamp)
    VALUES (NEW.block_height, NEW.timestamp)
    ON CONFLICT (block_height) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Optimized trigger function for address balance updates
CREATE OR REPLACE FUNCTION update_address_balance()
RETURNS TRIGGER AS $$
DECLARE
    prev_balance NUMERIC(20, 8);
    prev_block_height INTEGER;
BEGIN
    -- Get previous balance from materialized view
    SELECT balance, block_height
    INTO prev_balance, prev_block_height
    FROM latest_address_balances
    WHERE address = NEW.address;

    IF prev_balance IS NULL THEN
        prev_balance := 0;
        prev_block_height := -1;
    END IF;

    IF NEW.block_height > prev_block_height THEN
        INSERT INTO balance_address_blocks (
            address,
            block_height,
            balance,
            block_timestamp
        )
        VALUES (
            NEW.address,
            NEW.block_height,
            prev_balance + NEW.balance_delta,
            NEW.block_timestamp
        );

        -- Refresh materialized view concurrently
        REFRESH MATERIALIZED VIEW CONCURRENTLY latest_address_balances;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
DROP TRIGGER IF EXISTS trg_process_balance_changes ON transactions;
CREATE TRIGGER trg_process_balance_changes
    AFTER INSERT ON transactions
    FOR EACH ROW
    EXECUTE FUNCTION process_transaction_balance_changes();

DROP TRIGGER IF EXISTS trg_process_block ON transactions;
CREATE TRIGGER trg_process_block
    AFTER INSERT ON transactions
    FOR EACH ROW
    EXECUTE FUNCTION process_block_data();

DROP TRIGGER IF EXISTS trg_update_address_balance ON balance_changes;
CREATE TRIGGER trg_update_address_balance
    AFTER INSERT ON balance_changes
    FOR EACH ROW
    EXECUTE FUNCTION update_address_balance();

-- Optimized backfill function with batching
CREATE OR REPLACE FUNCTION backfill_address_balances(
    start_height INTEGER,
    end_height INTEGER,
    batch_size INTEGER DEFAULT 1000
)
RETURNS void AS $$
DECLARE
    current_batch_start INTEGER;
BEGIN
    FOR current_batch_start IN
        SELECT generate_series(start_height, end_height, batch_size)
    LOOP
        INSERT INTO balance_address_blocks (
            address,
            block_height,
            balance,
            block_timestamp
        )
        SELECT
            bc.address,
            current_batch_start,
            COALESCE((
                SELECT bab.balance
                FROM balance_address_blocks bab
                WHERE bab.address = bc.address
                AND bab.block_height < current_batch_start
                ORDER BY bab.block_height DESC
                LIMIT 1
            ), 0) + SUM(bc.balance_delta),
            MAX(bc.block_timestamp)
        FROM balance_changes bc
        WHERE bc.block_height BETWEEN
            current_batch_start
            AND LEAST(current_batch_start + batch_size - 1, end_height)
        GROUP BY bc.address;

        COMMIT;  -- Commit each batch
    END LOOP;
END;
$$ LANGUAGE plpgsql;