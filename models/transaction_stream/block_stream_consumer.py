import argparse
import os
import sys
import signal
import threading
from dotenv import load_dotenv
from loguru import logger
from models.balance_tracking.transaction_indexer import TransactionIndexer
from models.block_stream_consumer_base import BlockStreamConsumerBase
from models.block_stream_cursor import BlockStreamCursorManager
from typing import Dict, Any, List


class BlockStreamConsumer(BlockStreamConsumerBase):
    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 transaction_indexer: TransactionIndexer,
                 terminate_event,
                 partition: int = None,
                 is_live_mode: bool = False,
                 batch_size: int = 1000):

        super().__init__(
            kafka_config=kafka_config,
            block_stream_cursor_manager=block_stream_cursor_manager,
            terminate_event=terminate_event,
            consumer_name='transaction-stream-consumer',
            partition=partition,
            is_live_mode=is_live_mode,
            batch_size=batch_size
        )

        self.transaction_indexer = transaction_indexer

    def process_transactions(self, transactions: List[Dict]):
        self.transaction_indexer.index_transactions_in_batches(transactions)


if __name__ == "__main__":
    terminate_event = threading.Event()

    def shutdown_handler(signum, frame):
        logger.info("Shutdown signal received. Waiting for current processing to complete.")
        terminate_event.set()

    load_dotenv()

    parser = argparse.ArgumentParser(description='Bitcoin Block Stream Consumer')
    parser.add_argument('--live', action='store_true', default=False, help='Run in live mode')
    parser.add_argument('--partition', default=0, type=int, help='Partition number to process')
    args = parser.parse_args()

    is_live_mode = args.live
    if not is_live_mode and args.partition is None:
        parser.error("--partition is required when using --money-flow-archive")

    service_name = 'transaction-stream-consumer'

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

    connection_params = {
        "host": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_HOST", "localhost"),
        "port": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_PORT", "8123"),
        "database": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_DATABASE", "transaction_stream"),
        "user": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_USER", "default"),
        "password": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_PASSWORD", "changeit456$")
    }
    transaction_indexer = TransactionIndexer(connection_params)

    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'group.id': service_name,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.partition.fetch.bytes': 10485760,  # 10MB
        'fetch.max.bytes': 10485760,  # Modern setting replacing fetch.message.max.bytes
        'receive.message.max.bytes': 10486272  # Must be >= fetch.max.bytes + 512
    }

    try:
        consumer = BlockStreamConsumer(
            kafka_config,
            block_stream_cursor_manager,
            transaction_indexer,
            terminate_event,
            args.partition,
            is_live_mode,
            1
        )
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        block_stream_cursor_manager.close()
        logger.info("Balance indexer consumer stopped")
