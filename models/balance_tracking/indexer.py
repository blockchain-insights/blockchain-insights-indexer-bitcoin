import time
import signal
from node.node import BitcoinNode
from setup_logger import setup_logger
from setup_logger import logger_extra_data
from node.node_utils import parse_block_data
from models.balance_tracking.balance_indexer import BalanceIndexer


# Global flag to signal shutdown
shutdown_flag = False
logger = setup_logger("Indexer")


def shutdown_handler(signum, frame):
    global shutdown_flag
    logger.info(
        "Shutdown signal received. Waiting for current indexing to complete before shutting down."
    )
    shutdown_flag = True


def index_block(_bitcoin_node, _balance_indexer, block_height):
    block = _bitcoin_node.get_block_by_height(block_height)
    num_transactions = len(block["tx"])
    start_time = time.time()
    block_data = parse_block_data(block)
    success = _balance_indexer.create_rows_focused_on_balance_changes(block_data, _bitcoin_node)
    end_time = time.time()
    time_taken = end_time - start_time
    formatted_num_transactions = "{:>4}".format(num_transactions)
    formatted_time_taken = "{:6.2f}".format(time_taken)
    formatted_tps = "{:8.2f}".format(
        num_transactions / time_taken if time_taken > 0 else float("inf")
    )

    if time_taken > 0:
        logger.info(
            "Block Processed transactions",
            extra = logger_extra_data(
                block_height = f"{block_height:>6}",
                num_transactions = formatted_num_transactions,
                time_taken = formatted_time_taken,
                tps = formatted_tps,
            )
        )
    else:
        logger.info(
            "Block Processed transactions in 0.00 seconds (  Inf TPS).",
            extra = logger_extra_data(
                block_height = f"{block_height:>6}",
                num_transactions = formatted_num_transactions
            )
        )
        
    return success


def move_forward(_bitcoin_node, _balance_indexer, start_block_height = 0):
    global shutdown_flag

    skip_blocks = 6
    block_height = start_block_height
    
    while not shutdown_flag:
        current_block_height = _bitcoin_node.get_current_block_height() - skip_blocks
        if block_height > current_block_height:
            logger.info(f"Waiting for new blocks.", extra = logger_extra_data(current_block_height = current_block_height))
            time.sleep(10)
            continue
        
        success = index_block(_bitcoin_node, _balance_indexer, block_height)
        
        if success:
            block_height += 1
        else:
            logger.error(f"Failed to index block.", extra = logger_extra_data(block_height = block_height))
            time.sleep(30)

# Register the shutdown handler for SIGINT and SIGTERM
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    bitcoin_node = BitcoinNode()
    balance_indexer = BalanceIndexer()
    balance_indexer.setup_db()
    logger.info("Starting indexer")

    logger.info("Getting latest block number...")
    latest_block_height = balance_indexer.get_latest_block_number()
    logger.info(f"Latest block number", extra=logger_extra_data(latest_block_height = latest_block_height))
    
    move_forward(bitcoin_node, balance_indexer, latest_block_height + 1)

    balance_indexer.close()
    logger.info("Indexer stopped")
