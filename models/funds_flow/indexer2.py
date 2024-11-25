import datetime
import os
import sys
import signal
import threading
import time
from loguru import logger
from models.funds_flow.transaction_processor import TransactionProcessor, TransactionProcessorManager
from node.node import BitcoinNode
from node.node_utils import parse_block_data


class BitcoinTransactionIndexer:

    def __init__(self, bitcoin_node, transaction_processor, transaction_processor_manager, terminate_event: threading.Event):
        self.bitcoin_node = bitcoin_node
        self.transaction_processor = transaction_processor
        self.transaction_processor_manager = transaction_processor_manager
        self.terminate_event = terminate_event

    def index_block(self, block_height):
        block = self.bitcoin_node.get_block_by_height(block_height)
        num_transactions = len(block["tx"])
        start_time = time.time()
        block_data = parse_block_data(block)

        success = self.transaction_processor.publish_transaction(block_data)

        end_time = time.time()
        time_taken = end_time - start_time
        formatted_num_transactions = "{:>4}".format(num_transactions)
        formatted_time_taken = "{:6.2f}".format(time_taken)
        formatted_tps = "{:8.2f}".format(
            num_transactions / time_taken if time_taken > 0 else float("inf")
        )

        if time_taken > 0:
            logger.info("Processing transactions", block_height=f"{block_height:>6}", num_transactions=formatted_num_transactions, time_taken=formatted_time_taken, tps=formatted_tps)
        else:
            logger.info("Processed transactions in 0.00 seconds (  Inf TPS).", block_height=f"{block_height:>6}", num_transactions=formatted_num_transactions)

        return success

    def run(self, start_height: int):
        skip_blocks = 6
        forward_block_height = start_height

        while not self.terminate_event.is_set():
            current_block_height = self.bitcoin_node.get_current_block_height() - skip_blocks
            block_height = forward_block_height

            if block_height > current_block_height:
                logger.info(
                    f"Waiting for new blocks.",
                    block_height=current_block_height
                )
                time.sleep(10)
                continue

            while transaction_processor_manager.check_if_block_height_is_indexed(block_height):
                logger.info(f"Skipping block. Already indexed.", block_height=block_height)
                block_height += 1

            success = self.index_block(block_height)

            if success:
                transaction_processor_manager.add_block_height(block_height)
                forward_block_height = block_height + 1
            else:
                logger.error(f"Failed to index block.", block_height=block_height)
                time.sleep(30)


terminate_event = threading.Event()
def shutdown_handler(signum, frame):
    logger.info(
        "Shutdown signal received. Waiting for current indexing to complete before shutting down."
    )
    terminate_event.set()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    def patch_record(record):
        record["extra"]["service"] = 'bitcoin-transaction-indexer'
        return True

    logger.remove()
    logger.add(
        "../logs/bitcoin-transaction-indexer.log",
        rotation="500 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {extra}",
        level="DEBUG",
        filter=patch_record
    )

    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <blue>{message}</blue> | {extra}",
        level="DEBUG",
        filter=patch_record,
    )

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    db_url = os.getenv("REDPANDA_DB_CONNECTION_STRING", "postgresql://postgres:changeit456$@localhost:5420/redpanda")

    bitcoin_node = BitcoinNode()
    transaction_processor_manager = TransactionProcessorManager(db_url)
    kafka_config = {
        'bootstrap.servers': 'localhost:19092',  # Replace with your Redpanda/Kafka broker
        'group.id': 'bitcoin-transaction-consumer',
        'enable.idempotence': True,
        'auto.offset.reset': 'earliest'
    }
    transaction_processor = TransactionProcessor(kafka_config, bitcoin_node)
    logger.info("Starting indexer")

    bitcoin_transaction_indexer = BitcoinTransactionIndexer(bitcoin_node, transaction_processor, transaction_processor_manager, terminate_event)

    start_height = transaction_processor_manager.get_last_block_height() + 1
    bitcoin_transaction_indexer.run(start_height)

    logger.info("Indexer stopped")
