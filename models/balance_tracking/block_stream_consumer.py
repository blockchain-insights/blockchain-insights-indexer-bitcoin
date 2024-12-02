import os
import sys
import signal
import threading
from datetime import datetime
from typing import Dict, Any
import json
from loguru import logger
from confluent_kafka import Consumer
from models import BLOCK_STREAM_TOPIC_NAME
from models.balance_tracking import CONSUMER_NAME
from models.balance_tracking.transaction_indexer import TransactionIndexer
from models.block_stream_cursor import BlockStreamCursorManager


class BlockStreamConsumer:
    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 transaction_indexer: TransactionIndexer,
                 terminate_event):

        self.consumer = Consumer(kafka_config)
        self.block_stream_cursor_manager = block_stream_cursor_manager
        self.transaction_indexer = transaction_indexer
        self.terminate_event = terminate_event

    def on_assign(self, consumer, partitions):
        for partition in partitions:
            cursor = self.block_stream_cursor_manager.get_cursor(CONSUMER_NAME, partition.partition)
            if cursor is None:
                partition.offset = 0
            else:
                stored_offset = cursor.offset
                partition.offset = stored_offset + 1

        consumer.assign(partitions)

    def run(self):
        self.consumer.subscribe([BLOCK_STREAM_TOPIC_NAME], on_assign=self.on_assign)

        while not self.terminate_event.is_set():
            message = self.consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                logger.error("Consumer error", error=message.error())
                break

            self.process_message(message)

        self.consumer.close()

    def process_message(self, message):
        partition = message.partition()
        offset = message.offset()

        try:
            transaction = json.loads(message.value())

            self.transaction_indexer.index_transaction(tx=transaction)
            self.consumer.commit(message)
            self.block_stream_cursor_manager.set_cursor(
                CONSUMER_NAME,
                partition,
                offset,
                datetime.now()
            )

            logger.info("Processed transaction", block_height=transaction["block_height"], tx_id=transaction["tx_id"], partition=partition, offset=offset)

        except Exception as e:
            logger.error(
                "Error processing message",
                error=str(e),
                partition=partition,
                offset=offset
            )


if __name__ == "__main__":
    terminate_event = threading.Event()

    def shutdown_handler(signum, frame):
        logger.info(
            "Shutdown signal received. Waiting for current processing to complete."
        )
        terminate_event.set()


    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


    def patch_record(record):
        record["extra"]["service"] = 'balance-tracking-consumer'
        return True


    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <blue>{message}</blue> | {extra}",
        level="DEBUG",
        filter=patch_record,
    )

    logger.add(
        "logs/balance-tracking-consumer.log",
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

    block_stream_cursor_manager = BlockStreamCursorManager(db_url)

    timeseries_db_url = os.getenv("TIMESERIES_DB_CONNECTION_STRING", "postgresql://postgres:changeit456$@localhost:5432/timeseries")
    transaction_indexer = TransactionIndexer(timeseries_db_url)

    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'group.id': 'transaction-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'isolation.level': 'read_committed'
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

        # TESTING:
        # TODO: investigate 0 fee transfers... in early blocks they are present, but not in later blocks .. but i need to check if my system works correctly

        # BTC FULL NODE UPGRADE:
        # TODO: investigate https://bitcoincore.org/en/releases/28.0/
        # check if we can get spent utxo in a faster way?

