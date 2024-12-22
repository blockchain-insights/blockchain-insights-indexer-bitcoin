import os
import sys
import signal
import threading
from dotenv import load_dotenv
from loguru import logger
from models.balance_tracking.transaction_indexer import TransactionIndexer
from models.block_stream_consumer_base import LiveBlockStreamConsumer
from models.block_stream_cursor import BlockStreamCursorManager
from typing import Dict, Any, List


class BalanceTrackingConsumer(LiveBlockStreamConsumer):
    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 transaction_indexer: TransactionIndexer,
                 terminate_event,
                 batch_size: int = 1000):

        super().__init__(
            kafka_config=kafka_config,
            block_stream_cursor_manager=block_stream_cursor_manager,
            terminate_event=terminate_event,
            consumer_name='balance-tracking-consumer',
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

    service_name = 'balance-tracking-consumer'

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
        'max.partition.fetch.bytes': 134217728,
        'fetch.max.bytes': 134217728,
        'receive.message.max.bytes': 134218240,  # + 512 bytes
    }

    try:
        consumer = BalanceTrackingConsumer(
            kafka_config,
            block_stream_cursor_manager,
            transaction_indexer,
            terminate_event,
        )
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        block_stream_cursor_manager.close()
        logger.info("Indexer stopped")
