import os
import sys
import signal
import threading
from dotenv import load_dotenv
from loguru import logger
from models.balance_tracking.transaction_indexer import TransactionIndexer
from models.block_stream_consumer_base import BlockStreamConsumerBase
from models.block_stream_cursor import BlockStreamCursorManager
from typing import Dict, Any


class BlockStreamConsumer(BlockStreamConsumerBase):
    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 transaction_indexer: TransactionIndexer,
                 terminate_event):
        super().__init__(kafka_config, block_stream_cursor_manager, terminate_event, 0)
        self.transaction_indexer = transaction_indexer

    def index_transaction(self, tx):
        self.transaction_indexer.index_transaction(tx)


if __name__ == "__main__":
    terminate_event = threading.Event()

    def shutdown_handler(signum, frame):
        logger.info(
            "Shutdown signal received. Waiting for current processing to complete."
        )
        terminate_event.set()


    load_dotenv()

    service_name = 'transaction-consumer'

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


    def patch_record(record):
        record["extra"]["service"] = service_name
        return True


    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <blue>{message}</blue> | {extra}",
        level="DEBUG",
        filter=patch_record,
    )

    logger.add(
        f"../../logs/{service_name}.log",
        rotation="500 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {extra}",
        level="DEBUG",
        filter=patch_record
    )

    db_url = os.getenv(
        "REDPANDA_DB_CONNECTION_STRING",
        "postgresql://postgres:changeit456$@localhost:5420/block_stream"
    )
    redpanda_bootstrap_servers = os.getenv(
        "REDPANDA_BOOTSTRAP_SERVERS",
        "localhost:19092"
    )

    block_stream_cursor_manager = BlockStreamCursorManager(consumer_name=service_name, db_url=db_url)

    timeseries_db_url = os.getenv("TIMESERIES_DB_CONNECTION_STRING", "postgresql://postgres:changeit456$@localhost:5432/timeseries")
    transaction_indexer = TransactionIndexer(timeseries_db_url)

    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'group.id': service_name,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.partition.fetch.bytes': 10485760,  # 10MB - must match message.max.bytes
        'fetch.message.max.bytes': 10485760,  # 10MB - legacy setting, still good to set
        'receive.message.max.bytes': 10485760,  # 10MB - maximum size of messages the consumer can receive
    }

    try:
        consumer = BlockStreamConsumer(
            kafka_config,
            block_stream_cursor_manager,
            transaction_indexer,
            terminate_event
        )
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        block_stream_cursor_manager.close()
        logger.info("Balance indexer consumer stopped")
