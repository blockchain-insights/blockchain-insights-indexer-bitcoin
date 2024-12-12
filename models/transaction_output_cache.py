from typing import Tuple
import duckdb
from loguru import logger


class TransactionOutputCache:
    def __init__(self, csv_dir: str = 'csv', block_ranges: str = '1-99999'):
        self.conn = duckdb.connect(':memory:')
        self._initialize_tables(csv_dir, block_ranges)

    def _initialize_tables(self, csv_dir: str, block_ranges: str):
        # Load tx_out data from CSV
        self.conn.execute(f"""
            CREATE TABLE tx_outputs AS
            SELECT 
                txid,
                vout,
                addresses as address,
                value as amount
            FROM read_csv('{csv_dir}/tx_out-{block_ranges}.csv',
                         sep=';',
                         columns={{
                             'txid': 'VARCHAR',
                             'vout': 'INTEGER',
                             'value': 'BIGINT',
                             'scriptpubkey': 'VARCHAR',
                             'addresses': 'VARCHAR'
                         }},
                         header=False);
        """)
        
        # Create index for efficient lookups
        self.conn.execute("""
            CREATE INDEX idx_tx_outputs_lookup 
            ON tx_outputs(txid, vout);
        """)
        
        logger.info("Initialized transaction outputs table from CSV data")

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
