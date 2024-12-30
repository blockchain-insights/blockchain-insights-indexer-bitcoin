import time
import clickhouse_connect
from loguru import logger


class Storage:
    def __init__(self, connection_params):
        self.client = clickhouse_connect.get_client(
            host=connection_params.get('host', 'localhost'),
            port=int(connection_params.get('port', '8123')),  # Convert port to int
            username=connection_params.get('user', 'default'),  # Changed from username to user
            password=connection_params.get('password', ''),
            database=connection_params.get('database', 'default'),  # Changed from db to database
            settings={'max_execution_time': connection_params.get('max_execution_time', 3600)}  # Changed from max_execution_time to max_execution_time
        )
        self._init_table()

    def _init_table(self):
        self.client.command('''
            CREATE TABLE IF NOT EXISTS tx_outputs (
                txid_vout String,
                value Decimal(20,0),
                address String,
                _version UInt64
            ) ENGINE = ReplacingMergeTree(_version)
            PRIMARY KEY (txid_vout)
            ORDER BY txid_vout
        ''')

    def remove_duplicates(self):
        self.client.command('''OPTIMIZE TABLE tx_outputs DEDUPLICATE;''')

    def batch_insert(self, records):
        """Insert a batch of records into the database with proper error handling."""
        try:
            # Prepare data by joining txid and vout and adding current timestamp
            formatted_records = []
            current_timestamp = int(time.time())  # Get current Unix timestamp

            for txid, vout, value, address in records:
                txid_vout = f"{txid}-{vout}"
                formatted_records.append((txid_vout, value, address, current_timestamp))

            self.client.insert('tx_outputs',
                               formatted_records,
                               column_names=['txid_vout', 'value', 'address', '_version'])

        except Exception as e:
            logger.error(f"Database error during batch insert: {str(e)}")
            logger.error(f"First record in failed batch: {records[0] if records else 'No records'}")
            raise

    def get_output(self, txid_vout_pairs):
        """
        Get multiple outputs at once with batching
        txid_vout_pairs: list of tuples (txid, vout)
        Returns: dict mapping txid_vout to (address, value) tuples
        """
        if not txid_vout_pairs:
            return {}

        BATCH_SIZE = 500
        result_map = {}
        total_batches = (len(txid_vout_pairs) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_num, i in enumerate(range(0, len(txid_vout_pairs), BATCH_SIZE), 1):
            batch = txid_vout_pairs[i:i + BATCH_SIZE]
            txid_vouts = [f"{txid}-{vout}" for txid, vout in batch]

            try:
                result = self.client.query('''
                    SELECT txid_vout, address, value 
                    FROM tx_outputs 
                    WHERE txid_vout IN {txid_vouts:Array(String)}
                ''', parameters={'txid_vouts': txid_vouts})

                if result and result.result_rows:
                    batch_results = {row[0]: (row[1], row[2]) for row in result.result_rows}
                    result_map.update(batch_results)

            except Exception as e:
                logger.error("Error processing batch {}/{}: {}", batch_num, total_batches, str(e))
                continue

        return result_map
