from loguru import logger
from models import BLOCK_STREAM_TOPIC_NAME
from models.block_stream_cursor import BlockStreamCursorManager
from typing import Dict, Any, Optional, List
from confluent_kafka import Consumer, TopicPartition
from datetime import datetime
from collections import defaultdict
import json
import traceback


class BlockStreamConsumerBase:
    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 terminate_event,
                 consumer_name: str,
                 partition: Optional[int] = None,
                 is_live_mode: bool = False,
                 batch_size: int = 1000,
                 poll_timeout: float = 1.0):

        self.consumer = Consumer(kafka_config)
        self.block_stream_cursor_manager = block_stream_cursor_manager
        self.terminate_event = terminate_event
        self.consumer_name = consumer_name
        self.assigned_partition = partition
        self.is_live_mode = is_live_mode
        self.batch_size = batch_size
        self.poll_timeout = poll_timeout

        self.current_partition: Optional[int] = None
        self.last_processed_block = defaultdict(int)
        self.BLOCKS_PER_YEAR = 52560

    def get_partition_range(self, partition: int) -> tuple[int, int]:
        start = partition * self.BLOCKS_PER_YEAR
        end = (partition + 1) * self.BLOCKS_PER_YEAR - 1
        return start, end

    def initialize_partition_state(self, partition: int) -> int:
        cursor = self.block_stream_cursor_manager.get_cursor(partition)
        if cursor is None:
            return 0
        return cursor.offset + 1

    def check_next_partition(self, next_partition: int) -> bool:
        tp = TopicPartition(BLOCK_STREAM_TOPIC_NAME, next_partition, 0)
        low, high = self.consumer.get_watermark_offsets(tp)
        return high > 0

    def move_to_next_partition(self, next_partition: int):
        try:
            current_partition = self.current_partition
            self.current_partition = next_partition
            starting_offset = self.initialize_partition_state(next_partition)
            new_assignment = [TopicPartition(BLOCK_STREAM_TOPIC_NAME, next_partition, starting_offset)]
            self.consumer.assign(new_assignment)

            start_block, end_block = self.get_partition_range(next_partition)
            logger.info("Moving to next partition",
                        previous_partition=current_partition,
                        next_partition=next_partition,
                        starting_offset=starting_offset,
                        expected_block_range=(start_block, end_block))

        except Exception as e:
            logger.error("Error moving to next partition",
                         current_partition=current_partition,
                         traceback=traceback.format_exc(),
                         error=str(e))
            raise e

    def on_assign(self, consumer, partitions):
        if not self.is_live_mode:
            partition = TopicPartition(BLOCK_STREAM_TOPIC_NAME, self.assigned_partition,
                                       self.initialize_partition_state(self.assigned_partition))
            consumer.assign([partition])
            self.current_partition = self.assigned_partition
            logger.info(f"Archive mode: Assigned to partition {self.assigned_partition}")
            return

        # Live mode: Find the most recent partition with activity
        sorted_partitions = sorted(partitions, key=lambda p: p.partition)
        partition_offsets = {}

        for partition in sorted_partitions:
            starting_offset = self.initialize_partition_state(partition.partition)
            partition_offsets[partition.partition] = starting_offset

            if starting_offset > 0:
                # Found a partition with activity in the database
                self.current_partition = partition.partition
                partition.offset = starting_offset
                consumer.assign([partition])
                logger.info(
                    "Live mode: Resuming from database state",
                    partition=self.current_partition,
                    offset=starting_offset
                )
                return

        # If no previous state found, start with the earliest partition
        first_partition = sorted_partitions[0]
        self.current_partition = first_partition.partition
        consumer.assign([first_partition])
        logger.info(
            "Live mode: No previous state found, starting from first partition",
            partition=self.current_partition
        )

    def process_message_batch(self, messages: List) -> bool:
        """Process a batch of messages. Returns True if successful, False otherwise."""
        try:
            transactions = []
            latest_message = None

            # Prepare batch of transactions
            for message in messages:
                transaction = json.loads(message.value())
                transactions.append(transaction)
                latest_message = message  # Keep track of the latest message for committing

            # Process the batch of transactions
            self.process_transactions(transactions)

            # Update consumer state with the latest processed message
            if latest_message:
                self.consumer.commit(latest_message)
                self.block_stream_cursor_manager.set_cursor(
                    latest_message.partition(),
                    latest_message.offset(),
                    datetime.now()
                )
                self.last_processed_block[latest_message.partition()] = transactions[-1]["block_height"]

            logger.info("Processed batch of transactions",
                       batch_size=len(transactions),
                       last_block_height=transactions[-1]["block_height"] if transactions else None,
                       partition=latest_message.partition() if latest_message else None,
                       last_offset=latest_message.offset() if latest_message else None)

            return True

        except Exception as e:
            logger.error(
                "Error processing message batch",
                error=str(e),
                traceback=traceback.format_exc(),
                batch_size=len(messages)
            )
            return False

    def run(self):
        """Main processing loop with batch processing"""
        try:
            if self.is_live_mode:
                self.consumer.subscribe([BLOCK_STREAM_TOPIC_NAME], on_assign=self.on_assign)
            else:
                partition = TopicPartition(BLOCK_STREAM_TOPIC_NAME, self.assigned_partition, self.initialize_partition_state(self.assigned_partition))
                self.consumer.assign([partition])
                self.current_partition = self.assigned_partition

            consecutive_empty_polls = 0

            while not self.terminate_event.is_set():
                # Fetch multiple messages at once
                messages = self.consumer.consume(num_messages=self.batch_size, timeout=self.poll_timeout)

                if not messages:
                    consecutive_empty_polls += 1

                    if self.is_live_mode and consecutive_empty_polls >= 3 and self.last_processed_block[
                        self.current_partition] > 0:
                        _, end_block = self.get_partition_range(self.current_partition)
                        if self.last_processed_block[self.current_partition] >= end_block:
                            next_partition = self.current_partition + 1
                            if self.check_next_partition(next_partition):
                                self.move_to_next_partition(next_partition)
                    continue

                consecutive_empty_polls = 0

                # Filter messages for current partition and check for errors
                current_partition_messages = []
                for message in messages:
                    if message.error():
                        logger.error("Consumer error", error=message.error())
                        return
                    if message.partition() == self.current_partition:
                        current_partition_messages.append(message)

                # Process filtered messages if any
                if current_partition_messages:
                    if not self.process_message_batch(current_partition_messages):
                        break

        except Exception as e:
            logger.error("Fatal error in consumer run loop", error=str(e))
            raise
        finally:
            self.consumer.close()

    def process_transactions(self, transactions: List[Dict]):
        """
        Abstract method to be implemented by specific consumers.
        Processes a single transaction.
        """
        raise NotImplementedError("Subclasses must implement process_transaction")
