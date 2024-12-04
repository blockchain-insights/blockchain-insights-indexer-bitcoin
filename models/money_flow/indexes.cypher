CREATE INDEX ON :Transaction;
CREATE INDEX ON :Transaction(tx_id);
CREATE INDEX ON :Transaction(block_height);
CREATE INDEX ON :Transaction(out_total_amount);
CREATE INDEX ON :Address(address);

CREATE CONSTRAINT ON (t:Transaction) ASSERT t.tx_id IS UNIQUE;
CREATE CONSTRAINT ON (a:Address) ASSERT a.address IS UNIQUE;

CREATE EDGE INDEX ON :SENT(block_height);
