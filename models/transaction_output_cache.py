from typing import Tuple
import duckdb
from loguru import logger


class TransactionOutputCache:
    def __init__(self, csv_dir: str = '../csv', block_ranges: str = '1-99999'):
        self.conn = duckdb.connect(':memory:')
        self._initialize_tables(csv_dir, block_ranges)

    def _initialize_tables(self, csv_dir: str, block_ranges: str):
        # Load tx_out data
        self.conn.execute(f"""
            CREATE TABLE tx_out AS
            SELECT 
                txid,
                vout,
                addresses,
                value
            FROM read_csv('{csv_dir}/tx_out-{block_ranges}.csv',
                         sep=';',
                         columns={{
                             'txid': 'VARCHAR',
                             'vout': 'INTEGER',
                             'value': 'BIGINT',
                             'scriptpubkey': 'VARCHAR',
                             'addresses': 'VARCHAR'
                         }});
        """)

        # Load tx_in data
        self.conn.execute(f"""
            CREATE TABLE tx_in AS
            SELECT 
                txid,
                prev_txid,
                prev_vout
            FROM read_csv('{csv_dir}/tx_in-{block_ranges}.csv',
                         sep=';',
                         columns={{
                             'txid': 'VARCHAR',
                             'prev_txid': 'VARCHAR',
                             'prev_vout': 'INTEGER',
                             'scriptsig': 'VARCHAR',
                             'sequence': 'BIGINT'
                         }});
        """)
        
        # Create indexes for efficient joins
        self.conn.execute("CREATE INDEX idx_tx_out_lookup ON tx_out(txid, vout)")
        self.conn.execute("CREATE INDEX idx_tx_in_lookup ON tx_in(prev_txid, prev_vout)")
        
        logger.info("Initialized transaction tables from CSV data")

    def get_output(self, txid: str, vout_index: int) -> Tuple[str, int]:
        result = self.conn.execute("""
            SELECT 
                i.prev_txid,
                i.prev_vout,
                p_o.value as amount,
                p_o.addresses as address,
                i.txid as current_txid
            FROM tx_in i
            LEFT JOIN tx_out p_o 
                ON p_o.txid = i.prev_txid 
                AND p_o.vout = i.prev_vout
            WHERE i.txid = ?
            ORDER BY i.prev_vout
        """, [txid]).fetchone()
        
        if result:
            return result[3], result[2]  # address, amount
        return f"unknown-{txid}", 0
