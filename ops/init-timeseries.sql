GRANT ALL PRIVILEGES ON SCHEMA public TO CURRENT_USER;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE TYPE tx_event_type AS ENUM ('coinbase', 'transfer');

CREATE TABLE IF NOT EXISTS transactions (
    tx_id TEXT NOT NULL,
    tx_index INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    block_height INTEGER NOT NULL,
    is_coinbase BOOLEAN NOT NULL,
    in_total_amount NUMERIC(20, 0) NOT NULL,
    out_total_amount NUMERIC(20, 0) NOT NULL,
    fee_amount NUMERIC(20, 0) NOT NULL,
    PRIMARY KEY (tx_id, block_height)
);

SELECT create_hypertable('transactions', 'block_height',
    chunk_time_interval => 52560);

CREATE TABLE IF NOT EXISTS transaction_inputs (
    id BIGSERIAL,
    tx_id TEXT NOT NULL,
    address VARCHAR(64) NOT NULL,
    amount NUMERIC(20, 0) NOT NULL,
    block_height INTEGER NOT NULL,
    PRIMARY KEY (block_height, id)
);

SELECT create_hypertable('transaction_inputs', 'block_height',
    chunk_time_interval => 52560);

CREATE TABLE IF NOT EXISTS transaction_outputs (
    id BIGSERIAL,
    tx_id TEXT NOT NULL,
    address VARCHAR(64) NOT NULL,
    amount NUMERIC(20, 0) NOT NULL,
    block_height INTEGER NOT NULL,
    PRIMARY KEY (block_height, id)
);

SELECT create_hypertable('transaction_outputs', 'block_height',
    chunk_time_interval => 52560);

CREATE TABLE IF NOT EXISTS balance_changes (
    address VARCHAR(64) NOT NULL,
    block_height INTEGER NOT NULL,
    event tx_event_type NOT NULL,
    balance_delta NUMERIC(20, 0) NOT NULL,
    PRIMARY KEY (block_height, address)
);

SELECT create_hypertable('balance_changes', 'block_height',
    chunk_time_interval => 52560);

CREATE TABLE IF NOT EXISTS blocks (
    block_height INTEGER PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_timestamp ON blocks(timestamp);

CREATE TABLE IF NOT EXISTS balance_address_blocks (
    address VARCHAR(64) NOT NULL,
    block_height INTEGER NOT NULL,
    balance NUMERIC(20, 0) NOT NULL,
    PRIMARY KEY (address, block_height)
);

SELECT create_hypertable('balance_address_blocks', 'block_height',
    chunk_time_interval => 52560);

CREATE MATERIALIZED VIEW IF NOT EXISTS latest_address_balances AS
SELECT DISTINCT ON (address)
    address,
    block_height,
    balance
FROM balance_address_blocks
ORDER BY address, block_height DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_latest_balances ON latest_address_balances(address);

CREATE INDEX IF NOT EXISTS idx_inputs_addr_tx ON transaction_inputs (address, tx_id);
CREATE INDEX IF NOT EXISTS idx_outputs_addr_tx ON transaction_outputs (address, tx_id);
CREATE INDEX IF NOT EXISTS idx_bab_addr_height_lookup ON balance_address_blocks (address, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_bab_height_balance_top ON balance_address_blocks (block_height DESC, balance DESC) INCLUDE (address);
CREATE INDEX IF NOT EXISTS idx_bab_height_balance_range ON balance_address_blocks (block_height DESC, balance) INCLUDE (address);

CREATE OR REPLACE FUNCTION process_transaction(
    tx jsonb,
    tx_vins jsonb,
    tx_vouts jsonb
) RETURNS boolean AS $$
DECLARE
    inserted boolean;
BEGIN
    WITH tx_insert AS (
        INSERT INTO transactions (
            tx_id, tx_index, timestamp, block_height, is_coinbase,
            in_total_amount, out_total_amount, fee_amount
        )
        SELECT
            tx_id::text, tx_index, timestamp::timestamptz, block_height, is_coinbase,
            in_total_amount, out_total_amount,
            CASE WHEN is_coinbase THEN 0
                 ELSE in_total_amount - out_total_amount
            END as fee_amount
        FROM jsonb_to_record(tx) AS d(
            tx_id text, tx_index int, timestamp text,
            block_height int, is_coinbase boolean,
            in_total_amount numeric, out_total_amount numeric
        )
        ON CONFLICT (tx_id, block_height) DO NOTHING
        RETURNING tx_id, block_height
    ),
    input_insert AS (
        INSERT INTO transaction_inputs (tx_id, address, amount, block_height)
        SELECT
            d.tx_id,
            d.address,
            d.amount,
            t.block_height
        FROM jsonb_to_recordset(tx_vins) AS d(tx_id text, address varchar, amount numeric)
        CROSS JOIN tx_insert t
        WHERE EXISTS (SELECT 1 FROM tx_insert)
    ),
    output_insert AS (
        INSERT INTO transaction_outputs (tx_id, address, amount, block_height)
        SELECT
            d.tx_id,
            d.address,
            d.amount,
            t.block_height
        FROM jsonb_to_recordset(tx_vouts) AS d(tx_id text, address varchar, amount numeric)
        CROSS JOIN tx_insert t
        WHERE EXISTS (SELECT 1 FROM tx_insert)
    )
    SELECT EXISTS (SELECT 1 FROM tx_insert) INTO inserted;

    RETURN inserted;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION process_transaction_balance_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_coinbase THEN
        INSERT INTO balance_changes (address, block_height, event, balance_delta)
        SELECT
            address,
            NEW.block_height,
            'coinbase'::tx_event_type,
            amount
        FROM transaction_outputs
        WHERE tx_id = NEW.tx_id
        ON CONFLICT (block_height, address) DO UPDATE
        SET balance_delta = balance_changes.balance_delta + EXCLUDED.balance_delta;
    ELSE
        INSERT INTO balance_changes (address, block_height, event, balance_delta)
        SELECT
            address,
            NEW.block_height,
            'transfer'::tx_event_type,
            -amount
        FROM transaction_inputs
        WHERE tx_id = NEW.tx_id
        ON CONFLICT (block_height, address) DO UPDATE
        SET balance_delta = balance_changes.balance_delta + EXCLUDED.balance_delta;

        INSERT INTO balance_changes (address, block_height, event, balance_delta)
        SELECT
            address,
            NEW.block_height,
            'transfer'::tx_event_type,
            amount
        FROM transaction_outputs
        WHERE tx_id = NEW.tx_id
        ON CONFLICT (block_height, address) DO UPDATE
        SET balance_delta = balance_changes.balance_delta + EXCLUDED.balance_delta;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION process_block_data()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO blocks (block_height, timestamp)
    VALUES (NEW.block_height, NEW.timestamp)
    ON CONFLICT (block_height) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_address_balance()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO balance_address_blocks (address, block_height, balance)
    SELECT
        bc.address,
        NEW.block_height,
        COALESCE((
            SELECT balance
            FROM balance_address_blocks
            WHERE address = bc.address
            AND block_height < NEW.block_height
            ORDER BY block_height DESC
            LIMIT 1
        ), 0) + bc.balance_delta
    FROM balance_changes bc
    WHERE bc.address IN (
        SELECT address FROM transaction_inputs WHERE tx_id = NEW.tx_id
        UNION
        SELECT address FROM transaction_outputs WHERE tx_id = NEW.tx_id
    )
    AND bc.block_height = NEW.block_height
    ON CONFLICT (address, block_height) DO UPDATE
    SET balance = EXCLUDED.balance;

    REFRESH MATERIALIZED VIEW CONCURRENTLY latest_address_balances;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

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

DROP TRIGGER IF EXISTS trg_update_address_balance ON transactions;
CREATE TRIGGER trg_update_address_balance
    AFTER INSERT ON transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_address_balance();