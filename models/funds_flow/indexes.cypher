// Index for Transaction nodes by tx_id (unique constraint)
CREATE CONSTRAINT ON (t:Transaction) ASSERT t.tx_id IS UNIQUE;

// Index for Transaction nodes by block_height (non-unique)
CREATE INDEX ON :Transaction(block_height);

// Index for Transaction nodes by timestamp (non-unique)
CREATE INDEX ON :Transaction(timestamp);

// Index for Address nodes by address (unique constraint)
CREATE CONSTRAINT ON (a:Address) ASSERT a.address IS UNIQUE;

// Composite index for SENT relationships between Addresses
CREATE INDEX ON ()-[r:SENT]-() PROPERTY (amount, block_height);