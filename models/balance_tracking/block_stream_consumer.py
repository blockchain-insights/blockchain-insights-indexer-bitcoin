import os
import sys
import signal
import threading
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, Optional
import json
from loguru import logger
from confluent_kafka import Consumer, TopicPartition
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

        # Track partition completion status
        self.partition_message_counts = defaultdict(int)
        self.MAX_MESSAGES_PER_PARTITION = 52560
        self.current_partition: Optional[int] = None

    def initialize_partition_state(self, partition: int) -> int:
        cursor = self.block_stream_cursor_manager.get_cursor(CONSUMER_NAME, partition)
        if cursor is None:
            self.partition_message_counts[partition] = 0
            return 0
        else:
            self.partition_message_counts[partition] = cursor.offset + 1
            return cursor.offset + 1

    def on_assign(self, consumer, partitions):
        sorted_partitions = sorted(partitions, key=lambda p: p.partition)
        for partition in sorted_partitions:
            starting_offset = self.initialize_partition_state(partition.partition)
            partition.offset = starting_offset

        incomplete_partitions = [p.partition for p in sorted_partitions
                                 if self.partition_message_counts[p.partition] < self.MAX_MESSAGES_PER_PARTITION]

        if incomplete_partitions:
            self.current_partition = min(incomplete_partitions)
            current_partitions = [p for p in sorted_partitions if p.partition == self.current_partition]
            consumer.assign(current_partitions)

            logger.info("Starting consumption",
                        partition=self.current_partition,
                        starting_offset=self.partition_message_counts[self.current_partition],
                        max_messages=self.MAX_MESSAGES_PER_PARTITION)
        else:
            logger.info("No incomplete partitions found")
            consumer.assign([])

    def run(self):
        self.consumer.subscribe([BLOCK_STREAM_TOPIC_NAME], on_assign=self.on_assign)

        while not self.terminate_event.is_set():
            message = self.consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                logger.error("Consumer error", error=message.error())
                break

            if message.partition() == self.current_partition:
                self.process_message(message)
                if self.partition_message_counts[self.current_partition] >= self.MAX_MESSAGES_PER_PARTITION:
                    self.move_to_next_partition()

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

    def move_to_next_partition(self):
        try:
            incomplete_partitions = [p for p, count in self.partition_message_counts.items()
                                     if count < self.MAX_MESSAGES_PER_PARTITION]

            current_partition = self.current_partition

            if incomplete_partitions:
                next_partition = min(p for p in incomplete_partitions if p > self.current_partition)
                self.current_partition = next_partition
                starting_offset = self.initialize_partition_state(next_partition)
                new_assignment = [TopicPartition(BLOCK_STREAM_TOPIC_NAME, next_partition, starting_offset)]
                self.consumer.assign(new_assignment)
                logger.info("Moving to next partition",
                            previous_partition=current_partition,
                            next_partition=next_partition,
                            starting_offset=starting_offset,
                            remaining_partitions=len(incomplete_partitions) - 1)
            else:
                logger.info("All partitions completed", final_partition=current_partition)
                self.current_partition = None
                self.consumer.assign([])

        except Exception as e:
            logger.error("Error moving to next partition", current_partition=current_partition, error=str(e))
            raise

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
