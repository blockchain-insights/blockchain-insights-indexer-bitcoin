from loguru import logger
from typing import Dict, Any, Optional, List
from confluent_kafka import Consumer, TopicPartition
from datetime import datetime
from collections import defaultdict
import json
import traceback
from abc import ABC, abstractmethod

from models import BLOCK_STREAM_TOPIC_NAME
from models.block_stream_cursor import BlockStreamCursorManager


class BlockStreamConsumerBase(ABC):
    """Base class for all block stream consumers with common functionality"""

    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 terminate_event,
                 consumer_name: str,
                 batch_size: int = 1000,
                 poll_timeout: float = 1.0):

        self.consumer = Consumer(kafka_config)
        self.block_stream_cursor_manager = block_stream_cursor_manager
        self.terminate_event = terminate_event
        self.consumer_name = consumer_name
        self.batch_size = batch_size
        self.poll_timeout = poll_timeout
        self.current_partition: Optional[int] = None
        self.last_processed_block = defaultdict(int)

    def initialize_partition_state(self, partition: Optional[int]) -> int:
        cursor = self.block_stream_cursor_manager.get_cursor(partition)
        if cursor is None:
            return 0
        return cursor.offset + 1

    def process_message_batch(self, messages: List) -> bool:
        """Process a batch of messages. Returns True if successful, False otherwise."""
        try:
            transactions = []
            latest_message = None

            for message in messages:
                transaction = json.loads(message.value())
                transactions.append(transaction)
                latest_message = message

            self.process_transactions(transactions)

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

    @abstractmethod
    def process_transactions(self, transactions: List[Dict]):
        """Process a batch of transactions"""
        pass

    @abstractmethod
    def run(self):
        """Main processing loop"""
        pass


class PartitionBasedConsumer(BlockStreamConsumerBase):
    """Consumer that processes a specific partition"""

    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 terminate_event,
                 consumer_name: str,
                 partition: int,
                 batch_size: int = 1000,
                 poll_timeout: float = 1.0):

        super().__init__(kafka_config, block_stream_cursor_manager, terminate_event,
                         consumer_name, batch_size, poll_timeout)
        self.assigned_partition = partition

    def run(self):
        try:
            partition = TopicPartition(BLOCK_STREAM_TOPIC_NAME,
                                       self.assigned_partition,
                                       self.initialize_partition_state(self.assigned_partition))
            self.consumer.assign([partition])
            self.current_partition = self.assigned_partition

            consecutive_empty_polls = 0

            while not self.terminate_event.is_set():
                messages = self.consumer.consume(num_messages=self.batch_size,
                                                 timeout=self.poll_timeout)

                if not messages:
                    consecutive_empty_polls += 1
                    if consecutive_empty_polls >= 100:
                        logger.info("Archive mode: No new messages, shutting down")
                        break
                    if consecutive_empty_polls % 10 == 0:
                        logger.debug("No messages received")
                    continue

                consecutive_empty_polls = 0

                current_partition_messages = []
                for message in messages:
                    if message.error():
                        logger.error("Consumer error", error=message.error())
                        return
                    if message.partition() == self.current_partition:
                        current_partition_messages.append(message)

                if current_partition_messages:
                    if not self.process_message_batch(current_partition_messages):
                        break

        except Exception as e:
            logger.error("Fatal error in consumer run loop", error=str(e))
            raise
        finally:
            self.consumer.close()


class LiveBlockStreamConsumer(BlockStreamConsumerBase):
    """Consumer that processes blocks live and can move between partitions"""

    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 terminate_event,
                 consumer_name: str,
                 batch_size: int = 1000,
                 poll_timeout: float = 1.0):

        super().__init__(kafka_config, block_stream_cursor_manager, terminate_event,
                         consumer_name, batch_size, poll_timeout)
        self.BLOCKS_PER_YEAR = 52560

    def get_partition_range(self, partition: int) -> tuple[int, int]:
        start = partition * self.BLOCKS_PER_YEAR
        end = (partition + 1) * self.BLOCKS_PER_YEAR - 1
        return start, end

    def check_next_partition(self, next_partition: int) -> bool:
        tp = TopicPartition(BLOCK_STREAM_TOPIC_NAME, next_partition, 0)
        low, high = self.consumer.get_watermark_offsets(tp)
        return high > 0

    def find_starting_partition(self) -> tuple[int, int]:
        """Find the most appropriate starting partition and offset"""
        # Get metadata about all partitions
        cluster_metadata = self.consumer.list_topics(BLOCK_STREAM_TOPIC_NAME)
        topic_metadata = cluster_metadata.topics[BLOCK_STREAM_TOPIC_NAME]
        partitions = sorted(topic_metadata.partitions.keys())

        latest_partition = None
        latest_offset = -1

        # Check each partition for activity and stored cursor
        for partition in partitions:
            offset = self.initialize_partition_state(partition)
            if offset > latest_offset:
                latest_offset = offset
                latest_partition = partition

        if latest_partition is not None and latest_offset > 0:
            # We found a partition with previous activity
            return latest_partition, latest_offset
        else:
            # Find the first partition with messages
            for partition in partitions:
                if self.check_next_partition(partition):
                    return partition, 0

        # Fallback to first partition if nothing else found
        return partitions[0], 0

    def move_to_next_partition(self, next_partition: int):
        try:
            current_partition = self.current_partition
            self.current_partition = next_partition
            starting_offset = self.initialize_partition_state(next_partition)
            new_assignment = [TopicPartition(BLOCK_STREAM_TOPIC_NAME,
                                             next_partition, starting_offset)]
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

    def run(self):
        try:
            # Initialize with a specific partition instead of using subscribe
            starting_partition, starting_offset = self.find_starting_partition()
            self.current_partition = starting_partition

            # Explicitly assign the partition
            partition = TopicPartition(BLOCK_STREAM_TOPIC_NAME, starting_partition, starting_offset)
            self.consumer.assign([partition])

            logger.info("Starting live consumer",
                        partition=starting_partition,
                        offset=starting_offset)

            consecutive_empty_polls = 0

            while not self.terminate_event.is_set():
                messages = self.consumer.consume(num_messages=self.batch_size,
                                                 timeout=self.poll_timeout)

                if not messages:
                    consecutive_empty_polls += 1
                    if consecutive_empty_polls >= 3 and self.last_processed_block[self.current_partition] > 0:
                        _, end_block = self.get_partition_range(self.current_partition)
                        if self.last_processed_block[self.current_partition] >= end_block:
                            next_partition = self.current_partition + 1
                            if self.check_next_partition(next_partition):
                                self.move_to_next_partition(next_partition)
                                consecutive_empty_polls = 0

                    if consecutive_empty_polls % 10 == 0:
                        logger.debug("No messages received")
                    continue

                consecutive_empty_polls = 0

                current_partition_messages = []
                for message in messages:
                    if message.error():
                        logger.error("Consumer error", error=message.error())
                        return
                    if message.partition() == self.current_partition:
                        current_partition_messages.append(message)

                if current_partition_messages:
                    if not self.process_message_batch(current_partition_messages):
                        break

        except Exception as e:
            logger.error("Fatal error in consumer run loop", error=str(e))
            raise
        finally:
            self.consumer.close()