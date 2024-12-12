from typing import Tuple
import duckdb
from loguru import logger


class TransactionOutputCache:
    def __init__(self, db_path: str = ':memory:'):
        self.conn = duckdb.connect(db_path)
        self._initialize_tables()

    def _initialize_tables(self):
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
        result = self.conn.execute("""
            SELECT address, amount 
            FROM tx_outputs
            WHERE txid = ? AND vout = ?
            LIMIT 1
        """, [txid, vout_index]).fetchone()
        
        if result:
            return result[0], result[1]
        return f"unknown-{txid}", 0
