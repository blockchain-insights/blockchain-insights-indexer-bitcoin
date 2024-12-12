import sys
import signal
import threading
import duckdb
import json

from loguru import logger

blocks_csv = 'csv/blocks-1-99999.csv'
transactions_csv = 'csv/transactions-1-99999.csv'
tx_in_csv = 'csv/tx_in-1-99999.csv'
tx_out_csv = 'csv/tx_out-1-99999.csv'

if __name__ == "__main__":
    # Setup logging
    logger.remove()
    logger.add(
        "bitcoin-block-stream-archive.log",
        rotation="500 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {extra}",
        level="DEBUG"
    )
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message} | {extra}",
        level="DEBUG",
    )

    # Setup shutdown handling
    terminate_event = threading.Event()


    def shutdown_handler(signum, frame):
        logger.info("Shutdown signal received. Waiting for current processing to complete...")
        terminate_event.set()


    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    con = duckdb.connect()

    # Create tables with updated schemas
    # blocks: hash;height;version;size;prev_block_hash;merkleroot;time;bits;nonce
    con.execute(f"""
        CREATE OR REPLACE TABLE blocks AS
        SELECT * FROM read_csv('{blocks_csv}',
          delim=';',
          quote='',
          escape='',
          header=false,
          auto_detect=false,
          columns={{
            'hash': 'VARCHAR',
            'height': 'INTEGER',
            'version': 'INTEGER',
            'size': 'INTEGER',
            'prev_block_hash': 'VARCHAR',
            'merkleroot': 'VARCHAR',
            'time': 'INTEGER',
            'bits': 'INTEGER',
            'nonce': 'BIGINT'
          }}
        );
    """)

    # transactions: txid;block_hash;version;index
    con.execute(f"""
        CREATE OR REPLACE TABLE transactions AS
        SELECT * FROM read_csv('{transactions_csv}',
          delim=';',
          quote='',
          escape='',
          header=false,
          auto_detect=false,
          columns={{
            'txid': 'VARCHAR',
            'block_hash': 'VARCHAR',
            'version': 'INTEGER',
            'index': 'INTEGER'
          }}
        );
    """)

    # tx_in: txid;prev_txid;prev_vout;scriptsig;sequence
    con.execute(f"""
        CREATE OR REPLACE TABLE tx_in AS
        SELECT * FROM read_csv('{tx_in_csv}',
          delim=';',
          quote='',
          escape='',
          header=false,
          auto_detect=false,
          columns={{
            'txid': 'VARCHAR',
            'prev_txid': 'VARCHAR',
            'prev_vout': 'BIGINT',
            'scriptsig': 'VARCHAR',
            'sequence': 'BIGINT'
          }}
        );
    """)

    # tx_out: txid;vout;value;scriptpubkey;addresses
    con.execute(f"""
        CREATE OR REPLACE TABLE tx_out AS
        SELECT * FROM read_csv('{tx_out_csv}',
          delim=';',
          quote='',
          escape='',
          header=false,
          auto_detect=false,
          columns={{
            'txid': 'VARCHAR',
            'vout': 'INTEGER',
            'value': 'BIGINT',
            'scriptpubkey': 'VARCHAR',
            'addresses': 'VARCHAR'
          }}
        );
    """)

    # Try creating indexes
    try:
        con.execute("CREATE INDEX idx_blocks_hash ON blocks(hash)")
        con.execute("CREATE INDEX idx_transactions_txid ON transactions(txid)")
        con.execute("CREATE INDEX idx_transactions_block_hash ON transactions(block_hash)")
        con.execute("CREATE INDEX idx_tx_in_txid ON tx_in(txid)")
        con.execute("CREATE INDEX idx_tx_out_txid ON tx_out(txid)")
    except Exception as e:
        logger.warning("Index creation failed or not supported: {}", e)

    # Query to get transactions joined with blocks
    tx_query = """
    SELECT 
        t.txid AS tx_id,
        t.index AS tx_index,
        b.height AS block_height,
        b.time AS timestamp
    FROM transactions t
    JOIN blocks b ON t.block_hash = b.hash
    ORDER BY b.height, t.index;
    """

    transactions = con.execute(tx_query).fetchall()
    
    # Process each transaction
    for tx in transactions:
        if terminate_event.is_set():
            break

        tx_id, tx_index, block_height, timestamp = tx
        
        if terminate_event.is_set():
            break

        # Query outputs for this transaction and join with spending transactions
        vouts_query = """
        SELECT 
            o.txid,
            o.vout as out_index,
            o.value as amount,
            o.addresses as address,
            i.txid as spending_tx_id
        FROM tx_out o
        LEFT JOIN tx_in i 
            ON i.prev_txid = o.txid 
            AND i.prev_vout = o.vout
        WHERE o.txid = ?
        ORDER BY o.vout;
        """
        vouts = con.execute(vouts_query, [tx_id]).fetchall()
        
        # Query inputs and their referenced outputs with proper joining
        vins_query = """
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
        ORDER BY i.prev_vout;
        """
        vins = con.execute(vins_query, [tx_id]).fetchall()

        # Check if transaction is coinbase
        coinbase_query = """
        SELECT COUNT(*) = 1 AND COUNT(*) = (
            SELECT COUNT(*)
            FROM tx_in
            WHERE txid = ? AND prev_txid = ? AND prev_vout = 4294967295
        )
        FROM tx_in
        WHERE txid = ?;
        """
        is_coinbase = con.execute(coinbase_query, [tx_id, '0' * 64, tx_id]).fetchone()[0]

        # Process outputs
        # Debug print raw vouts data
        logger.debug(f"Raw VOUT data for tx {tx_id}: {vouts}")
        
        vouts_list = [
            {
                "address": vout[3] if vout[3] else None,
                "amount": vout[2] if vout[2] else 0,
                "tx_id": vout[4]  # spending_tx_id from the join
            } for vout in vouts
        ]
        
        # Debug print processed vouts
        logger.debug(f"Processed VOUTs for tx {tx_id}: {vouts_list}")

        # Process inputs
        if is_coinbase:
            vins_list = []  # Coinbase transactions have no real inputs
        else:
            vins_list = [
                {
                    "address": vin[3] if vin[3] else None,
                    "amount": vin[2] if vin[2] else 0,
                    "tx_id": vin[0] if vin[0] else None,
                    "vout": vin[1] if vin[1] else None
                } for vin in vins
            ]

        # Query to check if transaction is coinbase (moved up)
        coinbase_query = """
        SELECT COUNT(*) = 1 AND COUNT(*) = (
            SELECT COUNT(*)
            FROM tx_in
            WHERE txid = ? AND prev_txid = ? AND prev_vout = 4294967295
        )
        FROM tx_in
        WHERE txid = ?;
        """
        is_coinbase = con.execute(coinbase_query, [tx_id, '0' * 64, tx_id]).fetchone()[0]

        # Calculate totals
        in_total_amount = sum(vin["amount"] for vin in vins_list)
        out_total_amount = sum(vout["amount"] for vout in vouts_list)

        # If coinbase and no input amount, set in_total_amount = out_total_amount
        if is_coinbase and in_total_amount == 0:
            in_total_amount = out_total_amount

        # Format final message
        message = {
            "tx_id": tx_id,
            "tx_index": tx_index,
            "timestamp": timestamp,
            "block_height": block_height,
            "is_coinbase": is_coinbase,
            "in_total_amount": in_total_amount,
            "out_total_amount": out_total_amount,
            "vins": [
                {
                    "address": vin["address"],
                    "amount": vin["amount"],
                    "tx_id": vin["tx_id"]
                } for vin in vins_list
            ],
            "vouts": [
                {
                    "address": vout["address"],
                    "amount": vout["amount"],
                    "tx_id": vout["tx_id"]
                } for vout in vouts_list
            ],
            "size": None,
            "vsize": None,
            "weight": None
        }

        print(json.dumps(message))
        sys.stdout.flush()


    logger.info("Processing completed or terminated.")
