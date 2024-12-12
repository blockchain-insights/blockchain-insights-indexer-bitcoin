from typing import Tuple
import duckdb
from loguru import logger


class TransactionOutputCache:
    def __init__(self, db_path: str = ':memory:'):
        self.conn = duckdb.connect(db_path)
        self._initialize_tables()

    def _initialize_tables(self):
        """Initialize the database tables"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tx_outputs (
                txid VARCHAR,
                vout INTEGER,
                address VARCHAR,
                amount BIGINT,
                block_height INTEGER,
                PRIMARY KEY (txid, vout)
            )
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tx_outputs_lookup 
            ON tx_outputs(txid, vout)
        """)
        
        logger.info("Initialized transaction outputs table")

    def get_output(self, txid: str, vout_index: int) -> Tuple[str, int]:
        """Lookup transaction output by txid and vout index
        
        Args:
            txid: Transaction ID
            vout_index: Output index
            
        Returns:
            Tuple of (address, amount)
        """
        result = self.conn.execute("""
            SELECT address, amount 
            FROM tx_outputs
            WHERE txid = ? AND vout = ?
            LIMIT 1
        """, [txid, vout_index]).fetchone()
        
        if result:
            return result[0], result[1]
        return f"unknown-{txid}", 0

    def cache_transaction(self, block_height: int, tx: dict):
        """Cache a transaction's outputs
        
        Args:
            block_height: Block height
            tx: Transaction dict with vout array
        """
        if "vout" not in tx:
            logger.warning(f"Transaction {tx.get('txid', 'unknown')} has no outputs")
            return
            
        for vout in tx["vout"]:
            if vout.get("scriptPubKey", {}).get("type") in ["nonstandard", "nulldata"]:
                continue
                
            try:
                self.conn.execute("""
                    INSERT INTO tx_outputs (txid, vout, address, amount, block_height)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (txid, vout) DO UPDATE SET
                        address = excluded.address,
                        amount = excluded.amount,
                        block_height = excluded.block_height
                """, [
                    tx["txid"],
                    vout["n"],
                    vout["scriptPubKey"].get("address", f"unknown-{tx['txid']}"),
                    int(float(vout["value"]) * 100000000),
                    block_height
                ])
            except Exception as e:
                logger.error(f"Error caching output {tx['txid']}:{vout['n']}: {str(e)}")
