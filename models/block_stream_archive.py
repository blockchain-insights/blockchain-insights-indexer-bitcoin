import sys
import signal
import threading
import duckdb
import json
import time
import pandas as pd
import pyarrow as pa
from datetime import datetime
from loguru import logger

# Define input CSV and output Parquet paths
CSV_DIR = 'csv'
PARQUET_DIR = 'parquet'
BLOCK_RANGES = '1-99999'

blocks_parquet = f'{PARQUET_DIR}/blocks-{BLOCK_RANGES}.parquet'
transactions_parquet = f'{PARQUET_DIR}/transactions-{BLOCK_RANGES}.parquet'
tx_in_parquet = f'{PARQUET_DIR}/tx_in-{BLOCK_RANGES}.parquet'
tx_out_parquet = f'{PARQUET_DIR}/tx_out-{BLOCK_RANGES}.parquet'

def convert_csv_to_parquet():
    """Convert CSV files to Parquet format if they don't exist"""
    # Define schema types for each table
    blocks_dtypes = {
        'hash': str,
        'height': int,
        'version': int,
        'size': int,
        'prev_block_hash': str,
        'merkleroot': str,
        'time': int,
        'bits': int,
        'nonce': 'Int64'
    }
    
    transactions_dtypes = {
        'txid': str,
        'block_hash': str,
        'version': int,
        'index': int
    }
    
    tx_in_dtypes = {
        'txid': str,
        'prev_txid': str,
        'prev_vout': 'Int64',  # Using Int64 for large values like 4294967295
        'scriptsig': str,
        'sequence': 'Int64'
    }
    
    tx_out_dtypes = {
        'txid': str,
        'vout': int,
        'value': 'Int64',
        'scriptpubkey': str,
        'addresses': str
    }
    
    # Create parquet directory if it doesn't exist
    import os
    os.makedirs(PARQUET_DIR, exist_ok=True)
    
    # Convert each CSV to Parquet
    if not os.path.exists(blocks_parquet):
        df = pd.read_csv(f'{CSV_DIR}/blocks-{BLOCK_RANGES}.csv', 
                        names=blocks_dtypes.keys(),
                        dtype=blocks_dtypes,
                        header=None,
                        sep=';')
        df.to_parquet(blocks_parquet, index=False)
        
    if not os.path.exists(transactions_parquet):
        df = pd.read_csv(f'{CSV_DIR}/transactions-{BLOCK_RANGES}.csv',
                        names=transactions_dtypes.keys(),
                        dtype=transactions_dtypes,
                        header=None,
                        sep=';')
        df.to_parquet(transactions_parquet, index=False)
        
    if not os.path.exists(tx_in_parquet):
        df = pd.read_csv(f'{CSV_DIR}/tx_in-{BLOCK_RANGES}.csv',
                        names=tx_in_dtypes.keys(),
                        dtype=tx_in_dtypes,
                        header=None,
                        sep=';')
        df.to_parquet(tx_in_parquet, index=False)
        
    if not os.path.exists(tx_out_parquet):
        df = pd.read_csv(f'{CSV_DIR}/tx_out-{BLOCK_RANGES}.csv',
                        names=tx_out_dtypes.keys(),
                        dtype=tx_out_dtypes,
                        header=None,
                        sep=';')
        df.to_parquet(tx_out_parquet, index=False)

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

    con = duckdb.connect(":memory:")

    # Convert CSV files to Parquet if needed
    convert_csv_to_parquet()
    
    # Create tables from Parquet files
    con.execute(f"""
        CREATE OR REPLACE TABLE blocks AS
        SELECT * FROM parquet_scan('{blocks_parquet}');
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE transactions AS
        SELECT * FROM parquet_scan('{transactions_parquet}');
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE tx_in AS
        SELECT * FROM parquet_scan('{tx_in_parquet}');
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE tx_out AS
        SELECT * FROM parquet_scan('{tx_out_parquet}');
    """)

    # Create optimized indexes for joins
    try:
        con.execute("CREATE INDEX idx_blocks_hash ON blocks(hash)")
        con.execute("CREATE INDEX idx_transactions_txid ON transactions(txid)")
        con.execute("CREATE INDEX idx_transactions_block_hash ON transactions(block_hash)")
        con.execute("CREATE INDEX idx_tx_in_txid ON tx_in(txid)")
        con.execute("CREATE INDEX idx_tx_in_prev ON tx_in(prev_txid, prev_vout)")
        con.execute("CREATE INDEX idx_tx_out_txid ON tx_out(txid)")
        con.execute("CREATE INDEX idx_tx_out_composite ON tx_out(txid, vout)")

    except Exception as e:
        logger.warning("Index creation failed or not supported: {}", e)

    # Query to get all transactions
    tx_query = """
    SELECT DISTINCT
        t.txid as tx_id,
        t.index as tx_index,
        b.height as block_height,
        b.time as timestamp
    FROM transactions t
    JOIN blocks b ON t.block_hash = b.hash
    ORDER BY b.height, t.index
    """

    # SQL query templates for inputs and outputs
    vouts_query = """
        SELECT 
            txid as tx_id,
            vout as out_index,
            value as amount,
            addresses as address
        FROM tx_out
        WHERE txid = '{}'
        ORDER BY vout
    """

    vins_query = """
        SELECT 
            i.prev_txid,
            i.prev_vout,
            o.value as amount,
            o.addresses as address,
            i.txid as current_txid
        FROM tx_in i
        LEFT JOIN tx_out o ON o.txid = i.prev_txid AND o.vout = i.prev_vout
        WHERE i.txid = '{}'
        ORDER BY i.prev_vout
    """

    # Process transactions in batches
    BATCH_SIZE = 1000
    offset = 0
    tx_count = 0
    start_time = time.time()
    last_log_time = start_time
    
    while True:
        batch_query = f"SELECT * FROM ({tx_query}) AS sub LIMIT {BATCH_SIZE} OFFSET {offset}"
        transactions = con.execute(batch_query).fetchall()
        
        if not transactions:
            break
            
        for tx in transactions:
            if terminate_event.is_set():
                break

            tx_id, tx_index, block_height, timestamp = tx
            
            if terminate_event.is_set():
                break

            # Determine if this is a coinbase transaction (first transaction in block)
            is_coinbase = (tx_index == 0)

            # Execute queries for this transaction
            vouts = con.execute(vouts_query.format(tx_id)).fetchall()
            vins = con.execute(vins_query.format(tx_id)).fetchall()

            vouts_list = [
                {
                    "address": vout[3] if vout[3] else None,
                    "amount": vout[2] if vout[2] else 0,
                    "tx_id": vout[0]
                } for vout in vouts
            ]
            
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
            
            tx_count += 1
            if tx_count % 1000 == 0:
                current_time = time.time()
                elapsed = current_time - last_log_time
                tps = 1000 / elapsed if elapsed > 0 else 0
                total_elapsed = current_time - start_time
                avg_tps = tx_count / total_elapsed if total_elapsed > 0 else 0
                
                logger.info(
                    "Processed {} transactions. Current TPS: {:.2f}, Average TPS: {:.2f}",
                    tx_count, tps, avg_tps
                )
                last_log_time = current_time


        offset += BATCH_SIZE
        
    logger.info("Processing completed or terminated.")
