from loguru import logger
from models import BLOCK_STREAM_TOPIC_NAME
from models.block_stream_cursor import BlockStreamCursorManager
from typing import Dict, Any, Optional
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
                 start_partition: int):

        self.consumer = Consumer(kafka_config)
        self.block_stream_cursor_manager = block_stream_cursor_manager
        self.terminate_event = terminate_event

        self.current_partition: Optional[int] = None
        self.last_processed_block = defaultdict(int)

        self.BLOCKS_PER_YEAR = 52560
        self.start_partition = start_partition

    def get_partition_range(self, partition: int) -> tuple[int, int]:
        start = partition * self.BLOCKS_PER_YEAR
        end = (partition + 1) * self.BLOCKS_PER_YEAR - 1
        return start, end

    def initialize_partition_state(self, partition: int) -> int:
        cursor = self.block_stream_cursor_manager.get_cursor(partition)
        if cursor is None:
            return 0
        return cursor.offset + 1

    def on_assign(self, consumer, partitions):
        sorted_partitions = sorted(partitions, key=lambda p: p.partition)
        partition_offsets = {}
        for partition in sorted_partitions:
            starting_offset = self.initialize_partition_state(partition.partition)
            partition.offset = starting_offset
            partition_offsets[partition.partition] = starting_offset

            start_block, end_block = self.get_partition_range(partition.partition)
            logger.info("Partition range",
                        partition=partition.partition,
                        start_block=start_block,
                        end_block=end_block,
                        starting_offset=starting_offset)

        if not self.current_partition:
            active_partitions = [(p.partition, partition_offsets[p.partition])
                                 for p in sorted_partitions
                                 if partition_offsets[p.partition] > 0]

            if active_partitions:
                self.current_partition = max(active_partitions, key=lambda x: x[0])[0]
            else:
                self.current_partition = sorted_partitions[self.start_partition].partition

            current_partition = next(p for p in sorted_partitions
                                     if p.partition == self.current_partition)

            consumer.assign([current_partition])
            logger.info("Starting consumption",
                        partition=self.current_partition,
                        starting_offset=current_partition.offset,
                        selection_reason="highest_active" if active_partitions else "first_available")
            return

        logger.info("No partitions ready for processing")
        consumer.assign([])

    def check_next_partition(self, next_partition: int) -> bool:
        tp = TopicPartition(BLOCK_STREAM_TOPIC_NAME, next_partition, 0)
        low, high = self.consumer.get_watermark_offsets(tp)
        return high > 0

    def run(self):
        try:
            self.consumer.subscribe([BLOCK_STREAM_TOPIC_NAME], on_assign=self.on_assign)
            consecutive_empty_polls = 0
            while not self.terminate_event.is_set():
                message = self.consumer.poll(timeout=1.0)
                if message is None:
                    consecutive_empty_polls += 1

                    if consecutive_empty_polls >= 3 and self.last_processed_block[self.current_partition] > 0:
                        _, end_block = self.get_partition_range(self.current_partition)
                        if self.last_processed_block[self.current_partition] >= end_block:
                            next_partition = self.current_partition + 1
                            if self.check_next_partition(next_partition):
                                self.move_to_next_partition(next_partition)
                    continue

                consecutive_empty_polls = 0
                if message.error():
                    logger.error("Consumer error", error=message.error())
                    break

                if message.partition() == self.current_partition:
                    if not self.process_message(message):
                        break

        except Exception as e:
            logger.error("Fatal error in consumer run loop", error=str(e))
            raise
        finally:
            self.consumer.close()

    def process_message(self, message):
        partition = message.partition()
        offset = message.offset()

        try:
            transaction = json.loads(message.value())
            block_height = transaction["block_height"]

            self.index_transaction(tx=transaction)
            self.consumer.commit(message)
            self.block_stream_cursor_manager.set_cursor(
                partition,
                offset,
                datetime.now()
            )

            self.last_processed_block[partition] = block_height

            logger.info("Processed transaction",
                        block_height=block_height,
                        tx_id=transaction["tx_id"],
                        partition=partition,
                        offset=offset)

            return True
        except Exception as e:
            logger.error(
                "Error processing message",
                error=str(e),
                tbl=traceback.format_exc(),
                partition=partition,
                offset=offset
            )
            return False

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
                         tbl=traceback.format_exc(),
                         error=str(e))
            raise e

    def index_transaction(self, tx):
       pass
