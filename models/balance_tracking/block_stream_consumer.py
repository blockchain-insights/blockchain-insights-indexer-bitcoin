import os
import sys
import signal
import threading
import time
import json
import traceback
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient
from loguru import logger

from balance_indexer import BalanceIndexer


@dataclass
class Transaction:
    tx_id: str
    tx_index: int
    timestamp: int
    block_height: int
    is_coinbase: bool
    in_total_amount: int
    out_total_amount: int
    vins: List[Dict]
    vouts: List[Dict]


@dataclass
class BlockData:
    block_height: int
    timestamp: int
    transactions: List[Transaction]


class ConsumerStateManager:
    def __init__(self, db_url: str = None):
        if db_url is None:
            self.db_url = os.environ.get(
                "DB_CONNECTION_STRING",
                "postgresql://postgres:changeit456$@localhost:5432/miner"
            )
        else:
            self.db_url = db_url

        self.balance_indexer = BalanceIndexer(db_url)

    def get_last_processed_block(self) -> int:
        return self.balance_indexer.get_latest_block_number()

    def close(self):
        self.balance_indexer.close()


class BalanceIndexerConsumer:
    def __init__(
            self,
            kafka_config: dict,
            consumer_state_manager: ConsumerStateManager,
            terminate_event: threading.Event,
            topic_name: str = "transactions"
    ):
        self.consumer = Consumer(kafka_config)
        self.admin_client = AdminClient(kafka_config)
        self.consumer_state_manager = consumer_state_manager
        self.terminate_event = terminate_event
        self.topic_name = topic_name
        self.balance_indexer = consumer_state_manager.balance_indexer

        # Initialize transaction accumulator
        self.current_block_transactions: Dict[int, List[Transaction]] = {}

        # Batch processing settings
        self.max_batch_size = 100  # Number of blocks to process in one batch
        self.max_wait_time = 60  # Maximum seconds to wait before processing incomplete batch

    def _parse_transaction(self, transaction_data: dict) -> Transaction:
        return Transaction(
            tx_id=transaction_data["tx_id"],
            tx_index=transaction_data["tx_index"],
            timestamp=transaction_data["timestamp"],
            block_height=transaction_data["block_height"],
            is_coinbase=transaction_data["is_coinbase"],
            in_total_amount=transaction_data["in_total_amount"],
            out_total_amount=transaction_data["out_total_amount"],
            vins=transaction_data["vins"],
            vouts=transaction_data["vouts"]
        )

    def _process_accumulated_blocks(self):
        if not self.current_block_transactions:
            return

        block_heights = sorted(self.current_block_transactions.keys())
        block_data_list = []

        for height in block_heights:
            transactions = sorted(
                self.current_block_transactions[height],
                key=lambda x: x.tx_index
            )
            if transactions:
                block_data = BlockData(
                    block_height=height,
                    timestamp=transactions[0].timestamp,
                    transactions=transactions
                )
                block_data_list.append(block_data)

        if block_data_list:
            success = self.balance_indexer.create_rows_focused_on_balance_changes_batch(
                block_data_list,
                None  # Bitcoin node not needed as we have processed transaction data
            )

            if success:
                logger.info(
                    f"Successfully processed blocks",
                    start_height=block_heights[0],
                    end_height=block_heights[-1],
                    num_blocks=len(block_heights)
                )
                self.current_block_transactions.clear()
            else:
                logger.error(
                    f"Failed to process blocks",
                    start_height=block_heights[0],
                    end_height=block_heights[-1]
                )

    def _accumulate_transaction(self, transaction: Transaction):
        if transaction.block_height not in self.current_block_transactions:
            self.current_block_transactions[transaction.block_height] = []

        self.current_block_transactions[transaction.block_height].append(transaction)

        # Process if we have accumulated enough blocks
        if len(self.current_block_transactions) >= self.max_batch_size:
            self._process_accumulated_blocks()

    def seek_to_block_height(self, target_block_height: int):
        partitions = self.consumer.assignment()

        # Seek to beginning first to ensure we don't miss any messages
        for partition in partitions:
            self.consumer.seek(TopicPartition(self.topic_name, partition.partition, OFFSET_BEGINNING))

        # Skip messages until we reach our target block height
        while not self.terminate_event.is_set():
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                transaction_data = json.loads(msg.value())
                block_height = transaction_data["block_height"]

                if block_height >= target_block_height:
                    # Seek back one message so we process this one in the main loop
                    self.consumer.seek(TopicPartition(self.topic_name, msg.partition(), msg.offset()))
                    return
            except Exception as e:
                logger.error(f"Error processing message during seek: {e}")

    def run(self):
        try:
            self.consumer.subscribe([self.topic_name])

            # Get the last processed block height
            last_block_height = self.consumer_state_manager.get_last_processed_block()
            if last_block_height > 0:
                self.seek_to_block_height(last_block_height + 1)
                logger.info(f"Resuming from block height: {last_block_height + 1}")

            last_process_time = time.time()

            while not self.terminate_event.is_set():
                msg = self.consumer.poll(1.0)

                current_time = time.time()
                if current_time - last_process_time >= self.max_wait_time:
                    self._process_accumulated_blocks()
                    last_process_time = current_time

                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    transaction_data = json.loads(msg.value())
                    transaction = self._parse_transaction(transaction_data)
                    self._accumulate_transaction(transaction)

                except Exception as e:
                    logger.error(
                        f"Error processing message",
                        error=str(e),
                        traceback=traceback.format_exc()
                    )

        except Exception as e:
            logger.error(
                f"Fatal error in consumer",
                error=str(e),
                traceback=traceback.format_exc()
            )
        finally:
            self._process_accumulated_blocks()
            self.consumer.close()


def main():
    terminate_event = threading.Event()

    def shutdown_handler(signum, frame):
        logger.info(
            "Shutdown signal received. Waiting for current processing to complete."
        )
        terminate_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    def patch_record(record):
        record["extra"]["service"] = 'balance-indexer-consumer'
        return True

    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <blue>{message}</blue> | {extra}",
        level="DEBUG",
        filter=patch_record,
    )

    logger.add(
        "logs/balance-indexer-consumer.log",
        rotation="500 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {extra}",
        level="DEBUG",
        filter=patch_record
    )

    db_url = os.getenv(
        "REDPANDA_DB_CONNECTION_STRING",
        "postgresql://postgres:changeit456$@localhost:5420/redpanda"
    )
    redpanda_bootstrap_servers = os.getenv(
        "REDPANDA_BOOTSTRAP_SERVERS",
        "localhost:19092"
    )

    consumer_state_manager = ConsumerStateManager(db_url)

    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'group.id': 'balance-indexer-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
    }

    try:
        logger.info("Starting balance indexer consumer")
        consumer = BalanceIndexerConsumer(
            kafka_config,
            consumer_state_manager,
            terminate_event
        )
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        consumer_state_manager.close()
        logger.info("Balance indexer consumer stopped")


if __name__ == "__main__":
    main()