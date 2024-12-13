CREATE DATABASE IF NOT EXISTS transaction_stream;

USE transaction_stream;

-- Blocks table
CREATE TABLE blocks
(
    block_height UInt32,
    block_timestamp DateTime,
    _version UInt64,
    INDEX idx_time (block_timestamp) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY block_height
SETTINGS index_granularity = 8192;

-- Transactions table
CREATE TABLE transactions
(
    -- Block identification
    block_height UInt32,
    block_timestamp DateTime,

    -- Transaction details
    tx_id String,
    tx_index UInt32,
    is_coinbase Bool,

    -- Transaction metrics
    in_total_amount Decimal(20, 0),
    out_total_amount Decimal(20, 0),
    fee_amount Decimal(20, 0),
    size UInt32,
    vsize UInt32,
    weight UInt32,

    _version UInt64,

    -- Indexes for common queries
    INDEX idx_block_time (block_timestamp) TYPE minmax GRANULARITY 1,
    INDEX idx_block_height (block_height) TYPE minmax GRANULARITY 1,
    INDEX idx_tx_lookup (tx_id) TYPE set(100000) GRANULARITY 1,
    INDEX idx_fees (fee_amount) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_height, tx_index, tx_id)
SETTINGS index_granularity = 8192;

-- Time-based view for transactions
CREATE MATERIALIZED VIEW transactions_by_time
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_timestamp, block_height, tx_index)
POPULATE
AS SELECT * FROM transactions;

-- Transaction inputs table
CREATE TABLE transaction_inputs
(
    tx_id String,
    vin_index UInt32,
    block_height UInt32,
    address String,
    amount Decimal(20, 0),
    _version UInt64,
    INDEX idx_addr_tx (address, tx_id) TYPE set(100000) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(block_height, 52560)
ORDER BY (block_height, tx_id);

-- Transaction outputs table
CREATE TABLE transaction_outputs
(
    tx_id String,
    vout_index UInt32,
    block_height UInt32,
    address String,
    amount Decimal(20, 0),
    _version UInt64,
    INDEX idx_addr_tx (address, tx_id) TYPE set(100000) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(block_height, 52560)
ORDER BY (block_height, tx_id);

-- Balance changes table
CREATE TABLE balance_changes
(
    address String,
    block_height UInt32,
    tx_id String,
    tx_index UInt32,
    event Enum('coinbase' = 1, 'transfer' = 2),
    balance_delta Decimal(20, 0),
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(block_height, 52560)
ORDER BY (address, block_height, tx_index, event)
SETTINGS index_granularity = 8192;

-- Balance address blocks table
CREATE TABLE balance_address_blocks
(
    address String,
    block_height UInt32,
    balance Decimal(20, 0),
    _version UInt64,
    INDEX idx_balance_top (block_height, balance) TYPE minmax GRANULARITY 1,
    INDEX idx_balance_range (balance) TYPE minmax GRANULARITY 1,
    CONSTRAINT positive_balance CHECK balance >= 0
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(block_height, 52560)
ORDER BY (address, block_height)
SETTINGS index_granularity = 8192;