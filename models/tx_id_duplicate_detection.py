import os
import sys
import time
import json
import signal
import traceback
from loguru import logger
from dotenv import load_dotenv
from node.node import BitcoinNode
from node.storage import Storage


class DuplicateTransactionFinder:
    def __init__(self, bitcoin_node: BitcoinNode, output_file: str, state_file: str, batch_size: int = 100):
        self.bitcoin_node = bitcoin_node
        self.batch_size = batch_size
        self.output_file = output_file
        self.state_file = state_file
        self.tx_occurrences = {}
        self.total_duplicates = 0
        self.last_processed_height = self.load_state()
        self.should_exit = False

        # Create output file if it doesn't exist
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w') as f:
                f.write("txid,first_height,duplicate_height\n")

    def signal_handler(self, signum, frame):
        logger.info("Shutdown signal received. Finishing current batch...")
        self.should_exit = True

    def load_state(self) -> int:
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    height = state.get('last_processed_height', -1)
                    self.total_duplicates = state.get('total_duplicates', 0)
                    logger.info(f"Resuming from block height {height}")
                    logger.info(f"Previously found duplicates: {self.total_duplicates}")
                    return height
        except Exception as e:
            logger.error(f"Error loading state file: {e}")
        return -1

    def save_state(self, height: int):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                state = {
                    'last_processed_height': height,
                    'total_duplicates': self.total_duplicates,
                    'last_update': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'memory_usage': len(self.tx_occurrences)
                }

                # Write to temporary file first
                temp_file = f"{self.state_file}.tmp"
                with open(temp_file, 'w') as f:
                    json.dump(state, f, indent=2)

                # Rename temporary file to actual state file
                os.replace(temp_file, self.state_file)
                return True

            except Exception as e:
                logger.error(f"Error saving state file (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)

        return False

    def write_duplicate(self, txid: str, first_height: int, duplicate_height: int):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with open(self.output_file, 'a') as f:
                    f.write(f"{txid},{first_height},{duplicate_height}\n")
                return True
            except Exception as e:
                logger.error(f"Error writing duplicate (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
        return False

    def process_block_range(self, start_height: int, end_height: int) -> bool:
        try:
            blocks = self.bitcoin_node.get_blocks_by_height_range(start_height, end_height)

            if not blocks:
                logger.error(f"No blocks returned for range {start_height}-{end_height}")
                return False

            for block in blocks:
                if self.should_exit:
                    logger.info("Graceful shutdown requested, saving state...")
                    self.save_state(block["height"] - 1)
                    return False

                height = block["height"]
                for tx in block["tx"]:
                    txid = tx["txid"]

                    if txid in self.tx_occurrences:
                        first_height = self.tx_occurrences[txid]
                        self.write_duplicate(txid, first_height, height)
                        self.total_duplicates += 1
                    else:
                        self.tx_occurrences[txid] = height

                if height % 1000 == 0:
                    logger.info(f"Processed block {height}, found {self.total_duplicates} duplicate txids")
                    self.save_state(height)

                    # Memory cleanup
                    cutoff_height = height - 1000
                    self.tx_occurrences = {
                        txid: h for txid, h in self.tx_occurrences.items()
                        if h > cutoff_height
                    }

            if not self.should_exit:
                self.save_state(end_height)
            return not self.should_exit

        except Exception as e:
            logger.error(f"Error processing block range {start_height}-{end_height}: {str(e)}")
            logger.error(traceback.format_exc())
            return False


if __name__ == "__main__":
    load_dotenv()

    def patch_record(record):
        record["extra"]["service"] = 'duplicate-tx-finder'
        return True

    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <blue>{message}</blue> | {extra}",
        level="INFO",
        filter=patch_record,
    )
    logger.add(
        '../logs/duplicate_tx_finder.log',
        rotation="100 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {extra}",
        level="DEBUG",
        filter=patch_record
    )

    # Get configuration from environment
    bitcoin_node_rpc_url = os.getenv("BITCOIN_NODE_RPC_URL", "http://bitcoin-node:8332")
    output_file = os.getenv("OUTPUT_FILE", "duplicate_transactions.csv")
    state_file = os.getenv("STATE_FILE", "duplicate_finder_state.json")

    # Initialize components
    connection_params = {
        "host": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_HOST", "localhost"),
        "port": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_PORT", "8123"),
        "database": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_DATABASE", "transaction_stream"),
        "user": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_USER", "default"),
        "password": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_PASSWORD", "changeit456$"),
        "query_timeout": int(os.getenv("TRANSACTION_STREAM_CLICKHOUSE_QUERY_TIMEOUT", "1800")),
    }

    storage = Storage(connection_params)
    bitcoin_node = BitcoinNode(bitcoin_node_rpc_url, storage)
    finder = DuplicateTransactionFinder(bitcoin_node, output_file, state_file)

    # Setup signal handlers
    signal.signal(signal.SIGINT, finder.signal_handler)
    signal.signal(signal.SIGTERM, finder.signal_handler)

    try:
        current_height = bitcoin_node.get_current_block_height()
        start_time = time.time()

        start_height = finder.last_processed_height + 1 if finder.last_processed_height >= 0 else 0

        if start_height > 0:
            logger.info(f"Resuming from block {start_height}")
        logger.info(f"Will process up to block {current_height}")
        logger.info(f"Results will be written to {output_file}")

        batch_size = 100
        for batch_start in range(start_height, current_height + 1, batch_size):
            end_height = min(batch_start + batch_size - 1, current_height)

            if not finder.process_block_range(batch_start, end_height):
                if finder.should_exit:
                    logger.info("Gracefully shutting down...")
                    break

                logger.error(f"Failed to process batch {batch_start}-{end_height}")
                logger.info("Retrying in 30 seconds...")
                time.sleep(30)
                continue

            if batch_start % 1000 == 0:
                elapsed = time.time() - start_time
                # Calculate progress based on absolute blockchain height
                progress = (batch_start / current_height) * 100

                # Calculate ETA based on actual processed blocks
                blocks_processed = batch_start - start_height
                blocks_remaining = current_height - batch_start
                if blocks_processed > 0:
                    blocks_per_second = blocks_processed / elapsed
                    eta = blocks_remaining / blocks_per_second
                else:
                    eta = 0

                logger.info(
                    f"Progress: {progress:.2f}% | "
                    f"Block: {batch_start}/{current_height} | "
                    f"Duplicates found: {finder.total_duplicates} | "
                    f"ETA: {eta / 3600:.1f}h"
                )

        elapsed = time.time() - start_time
        logger.info(
            f"{'Completed' if not finder.should_exit else 'Stopped'} in {elapsed / 3600:.1f} hours | "
            f"Found {finder.total_duplicates} duplicate transactions | "
            f"Results saved to {output_file}"
        )

    except Exception as e:
        logger.error(f"Error running duplicate transaction finder: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)