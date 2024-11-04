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

def index_block_batch(_bitcoin_node, _balance_indexer, start_height: int, batch_size: int = 10):
    """Index by batch"""
    end_height = start_height + batch_size - 1

    blocks = _bitcoin_node.get_blocks_by_height_range(start_height, end_height)
    if not blocks:
        logger.error(f"Failed to fetch blocks",
                     extra=logger_extra_data(start_height=start_height, end_height=end_height))
        return False

    start_time = time.time()
    total_transactions = sum(len(block["tx"]) for block in blocks)

    block_data_list = [parse_block_data(block) for block in blocks]

    success = _balance_indexer.create_rows_focused_on_balance_changes_batch(block_data_list, _bitcoin_node)

    end_time = time.time()
    time_taken = end_time - start_time

    if success:
        logger.info(
            "Batch processed",
            extra=logger_extra_data(start_height=f"{start_height:>6}",
                                    end_height=f"{end_height:>6}",
                                    blocks=len(blocks),
                                    transactions=f"{total_transactions:>6}",
                                    time_taken=f"{time_taken:>6.2f}",
                                    tps=f"{total_transactions/time_taken:8.2f}" if time_taken > 0 else "inf",)
        )

    return success

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
    batch_size = 10
    block_height = start_block_height

    logger.info(f"Start block height: {start_block_height}")
    
    while not shutdown_flag:
        current_block_height = _bitcoin_node.get_current_block_height() - skip_blocks
        if block_height > current_block_height:
            logger.info(f"Waiting for new blocks.", extra = logger_extra_data(current_block_height = current_block_height))
            time.sleep(10)
            continue

        remaining_blocks = current_block_height - block_height + 1
        current_batch_size = min(batch_size, remaining_blocks)

        success = index_block_batch(_bitcoin_node, _balance_indexer, block_height, current_batch_size)
        
        if success:
            block_height += current_batch_size
        else:
            logger.error(f"Failed to index batch.", extra = logger_extra_data(block_height = block_height))
            time.sleep(30)

# Register the shutdown handler for SIGINT and SIGTERM
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    bitcoin_node = BitcoinNode()
    balance_indexer = BalanceIndexer()

    logger.info("Starting indexer")

    logger.info("Getting latest block number...")
    latest_indexed_block = balance_indexer.get_latest_block_number()
    start_height = 0 if latest_indexed_block == 0 else latest_indexed_block + 1
    
    move_forward(bitcoin_node, balance_indexer, start_height)

    balance_indexer.close()
    logger.info("Indexer stopped")
