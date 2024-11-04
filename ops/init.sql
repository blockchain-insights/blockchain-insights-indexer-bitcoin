-- Grant privileges on the public schema to the current user
GRANT ALL PRIVILEGES ON SCHEMA public TO CURRENT_USER;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create a static integer_now function
CREATE OR REPLACE FUNCTION block_height_now()
RETURNS INTEGER LANGUAGE SQL STABLE AS
$$
  SELECT 2147483647;  -- Maximum 32-bit integer
$$;

-- Create tables
CREATE TABLE IF NOT EXISTS balance_changes (
    address TEXT NOT NULL,
    block_height INTEGER NOT NULL,
    event TEXT,
    balance_delta BIGINT NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(address, block_height)
);

CREATE TABLE IF NOT EXISTS balance_address_blocks (
    address TEXT NOT NULL,
    block_height INTEGER NOT NULL,
    balance BIGINT NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(address, block_height)
);

CREATE TABLE IF NOT EXISTS blocks (
    block_height INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(block_height)
);

-- Convert to hypertables and set integer_now function
SELECT create_hypertable('balance_changes', 'block_height',
    chunk_time_interval => 12960,
    if_not_exists => TRUE,
    migrate_data => TRUE,
    create_default_indexes => FALSE
);

SELECT set_integer_now_func('balance_changes', 'block_height_now');

SELECT create_hypertable('balance_address_blocks', 'block_height',
    chunk_time_interval => 12960,
    if_not_exists => TRUE,
    migrate_data => TRUE,
    create_default_indexes => FALSE
);

SELECT set_integer_now_func('balance_address_blocks', 'block_height_now');

-- Create trigger function for automatic balance tracking
CREATE OR REPLACE FUNCTION update_balance_address_block()
    RETURNS TRIGGER AS $func$
    DECLARE
        _last_balance BIGINT;
        _last_block_height INTEGER;
    BEGIN
        -- Cache the last balance in a variable to avoid multiple lookups
        SELECT balance, block_height
        INTO _last_balance, _last_block_height
        FROM balance_address_blocks
        WHERE address = NEW.address
        AND block_height < NEW.block_height
        ORDER BY block_height DESC
        LIMIT 1;

        -- Insert new balance state
        INSERT INTO balance_address_blocks (
            address,
            block_height,
            block_timestamp,
            balance
        )
        VALUES (
            NEW.address,
            NEW.block_height,
            NEW.block_timestamp,
            COALESCE(_last_balance, 0) + NEW.balance_delta
        );

        RETURN NEW;
    END;
    $func$ LANGUAGE plpgsql;

-- Create the trigger
DROP TRIGGER IF EXISTS balance_changes_after_insert ON balance_changes;
CREATE TRIGGER balance_changes_after_insert
    AFTER INSERT ON balance_changes
    FOR EACH ROW
    EXECUTE FUNCTION update_balance_address_block();

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_balance_changes_block_height
    ON balance_changes (block_height);

CREATE INDEX IF NOT EXISTS idx_balance_changes_addr_time
    ON balance_changes (address, block_timestamp DESC);

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

CREATE INDEX IF NOT EXISTS idx_blocks_timestamp
    ON blocks (timestamp);


-- Create monitoring view
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_tables_stats AS
WITH stats AS (
    SELECT
        date_trunc('hour', block_timestamp) as time_bucket,
        COUNT(*) as total_records,
        COUNT(DISTINCT address) as unique_addresses,
        MIN(balance) as min_balance,
        MAX(balance) as max_balance,
        AVG(balance)::BIGINT as avg_balance,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY balance) as median_balance
    FROM balance_address_blocks
    GROUP BY date_trunc('hour', block_timestamp)
)
SELECT * FROM stats;

-- Create index on the materialized view
CREATE INDEX IF NOT EXISTS idx_balance_stats_time
    ON balance_tables_stats (time_bucket DESC);

-- Create refresh function
CREATE OR REPLACE FUNCTION refresh_balance_stats()
    RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY balance_tables_stats;
END;
$$ LANGUAGE plpgsql;
