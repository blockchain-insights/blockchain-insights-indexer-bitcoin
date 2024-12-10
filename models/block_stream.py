import os
import sys
import signal
import threading
import time
import json
import traceback
from decimal import Decimal
from confluent_kafka.admin import AdminClient
from confluent_kafka import admin
from confluent_kafka import Producer
from loguru import logger
from typing import List
from models.block_range_partitioner import BlockRangePartitioner
from models.block_stream_state import BlockStreamStateManager
from models.block_stream_utils import TransactionOutputCache
from node.node import BitcoinNode


class BlockStreamProducer:
    def __init__(self, kafka_config: dict, bitcoin_node, db_url: str, num_partitions: int = 39):
        self.producer = Producer(kafka_config)
        self.bitcoin_node = bitcoin_node
        self.topic_name = "transactions"
        self.num_partitions = num_partitions
        self.partitioner = BlockRangePartitioner(self.num_partitions)

        self.admin_client = AdminClient(kafka_config)
        self.ensure_topic_exists()

        self.tx_cache = TransactionOutputCache(
            db_url=db_url,
            bitcoin_node=bitcoin_node
        )

        self.duplicate_tx = [
            'd5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599',
            'e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468'
        ]
        self.duplicate_block = [91812, 91722]

    def ensure_topic_exists(self):
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            if self.topic_name not in metadata.topics:
                new_topic = admin.NewTopic(
                    self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=1,
                    config={'retention.ms': '-1', 'retention.bytes': '-1'}
                )

                fs = self.admin_client.create_topics([new_topic])
                for topic, future in fs.items():
                    try:
                        future.result()
                        logger.info(f"Created topic {topic} with infinite retention")
                    except Exception as e:
                        logger.error(f"Failed to create topic {topic}", error=e)
                        raise
            else:
                logger.info(f"Topic {self.topic_name} already exists")

        except Exception as e:
            logger.error(f"Error managing topic", error=e, trb=traceback.format_exc())
            raise

    def process_transaction(self, tx: dict, block: dict) -> dict:
        """Process a single transaction with cached lookups"""
        is_coinbase = len(tx["vin"]) == 1 and "coinbase" in tx["vin"][0]

        # Process inputs
        in_amount_by_address = {}
        if not is_coinbase:
            for vin in tx["vin"]:
                address, amount = self.tx_cache.get_output(vin["txid"], vin["vout"])
                in_amount_by_address[address] = in_amount_by_address.get(address, 0) + amount

        # Process outputs
        out_amount_by_address = {}
        for vout in tx["vout"]:
            if vout["scriptPubKey"].get("type") in ["nonstandard", "nulldata"]:
                continue

            address = self.tx_cache._derive_address(vout["scriptPubKey"])
            amount = int(Decimal(vout["value"]) * 100000000)
            out_amount_by_address[address] = out_amount_by_address.get(address, 0) + amount

        if not is_coinbase:
            for addr, amt in in_amount_by_address.items():
                if amt == 0:
                    raise ValueError(f"Transaction for vin was not found in cache: {tx['txid']}")

        return {
            "tx_id": tx["txid"],
            "tx_index": tx.get("index", 0),
            "timestamp": block["time"],
            "block_height": block["height"],
            "is_coinbase": is_coinbase,
            "in_total_amount": sum(in_amount_by_address.values()),
            "out_total_amount": sum(out_amount_by_address.values()),
            "vins": [
                {"address": addr, "amount": amt, "tx_id": tx["txid"]}
                for addr, amt in in_amount_by_address.items()
                if amt != 0
            ],
            "vouts": [
                {"address": addr, "amount": amt, "tx_id": tx["txid"]}
                for addr, amt in out_amount_by_address.items()
                if amt != 0
            ],
            "size": tx.get("size", 0),
            "vsize": tx.get("vsize", 0),
            "weight": tx.get("weight", 0)
        }

    def process_block(self, block: dict) -> List[dict]:
        """Process all transactions in a block"""
        try:
            transactions = []
            start_time = time.time()

            for tx in block["tx"]:
                if tx["txid"] in self.duplicate_tx and block['height'] in self.duplicate_block:
                    logger.warning(f"Skipping duplicate transaction {tx['txid']}")
                    continue

                transaction = self.process_transaction(tx, block)
                transactions.append(transaction)

            end_time = time.time()
            time_taken = end_time - start_time

            logger.info(
                "Processed block",
                block_height=block["height"],
                num_transactions=len(transactions),
                time_taken=f"{time_taken:.2f}s",
                tps=f"{len(transactions) / time_taken:.2f}" if time_taken > 0 else "∞"
            )

            return transactions

        except Exception as e:
            logger.error(
                "Failed to process block",
                block_height=block["height"],
                error=str(e),
                traceback=traceback.format_exc()
            )
            raise e

    def send_to_stream(self, transactions: List[dict]):
        """Send processed transactions to Kafka stream"""
        try:
            for tx in transactions:
                partition = self.partitioner(tx["block_height"])
                self.producer.produce(
                    topic=self.topic_name,
                    key=tx["tx_id"],
                    value=json.dumps(tx),
                    partition=partition,
                    timestamp=int(tx["timestamp"] * 1000)
                )
            self.producer.flush()

        except Exception as e:
            logger.error(
                "Failed to send transactions to stream",
                error=str(e),
                traceback=traceback.format_exc()
            )
            raise

    def close(self):
        """Cleanup resources"""
        try:
            if self.tx_cache:
                self.tx_cache.close()
            if self.producer:
                self.producer.flush()
            if self.admin_client:
                self.admin_client.close()
            logger.info("Closed BlockStreamProducer resources")
        except Exception as e:
            logger.error(f"Error closing BlockStreamProducer: {e}")
            raise

    def __del__(self):
        try:
            self.close()
        except:
            pass


class BlockStream:
    def __init__(
            self,
            bitcoin_node,
            producer: BlockStreamProducer,
            state_manager,
            terminate_event: threading.Event,
            window_size: int = 3
    ):
        self.bitcoin_node = bitcoin_node
        self.producer = producer
        self.state_manager = state_manager
        self.terminate_event = terminate_event
        self.window_size = window_size

    def process_window(self, start_height: int) -> bool:
        """Process a window of blocks"""
        try:
            # Get blocks for window
            end_height = start_height + self.window_size - 1
            blocks = self.bitcoin_node.get_blocks_by_height_range(start_height, end_height)

            if not blocks:
                return False

            for block in blocks:
                for tx in block["tx"]:
                    self.producer.tx_cache.cache_transaction(block['height'], tx)

            # Process each block in window
            for block in blocks:
                if self.state_manager.check_if_block_height_is_indexed(block["height"]):
                    logger.info(f"Skipping block. Already indexed.", block_height=block["height"])
                    continue

                transactions = self.producer.process_block(block)
                if transactions:
                    self.producer.send_to_stream(transactions)
                    self.state_manager.add_block_height(block["height"])

            return True

        except Exception as e:
            logger.error(
                "Failed to process block window",
                start_height=start_height,
                error=str(e),
                traceback=traceback.format_exc()
            )
            raise e

    def run(self, start_height: int):
        """Main processing loop"""
        skip_blocks = 6
        forward_block_height = start_height

        while not self.terminate_event.is_set():
            try:
                current_block_height = self.bitcoin_node.get_current_block_height() - skip_blocks

                if forward_block_height > current_block_height:
                    logger.info(
                        "Waiting for new blocks",
                        current_height=current_block_height,
                        next_height=forward_block_height
                    )
                    time.sleep(10)
                    continue

                success = self.process_window(forward_block_height)

                if success:
                    forward_block_height += self.window_size
                else:
                    logger.error(
                        "Failed to process window",
                        start_height=forward_block_height
                    )
                    time.sleep(30)

            except Exception as e:
                logger.error(
                    "Error in main processing loop",
                    error=str(e),
                    traceback=traceback.format_exc()
                )
                time.sleep(30)


# Main script section
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    def patch_record(record):
        record["extra"]["service"] = 'bitcoin-block-stream'
        return True

    logger.remove()
    logger.add(
        "../logs/bitcoin-block-stream.log",
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

    # Setup shutdown handling
    terminate_event = threading.Event()

    def shutdown_handler(signum, frame):
        logger.info("Shutdown signal received. Waiting for current processing to complete...")
        terminate_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Initialize components
    db_url = os.getenv("BLOCK_STREAM_DB_CONNECTION_STRING")
    redpanda_bootstrap_servers = os.getenv("BLOCK_STREAM_REDPANDA_BOOTSTRAP_SERVERS")
    bitcoin_node_rpc_url = os.getenv("BITCOIN_NODE_RPC_URL")

    bitcoin_node = BitcoinNode(bitcoin_node_rpc_url)
    state_manager = BlockStreamStateManager(db_url)

    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'enable.idempotence': True,
        'compression.type': 'zstd',
        'acks': 'all',
        'message.max.bytes': 10485760,  # 10MB
        'batch.size': 1000000,  # 1MB
        'linger.ms': 100,
        'compression.level': 9  # Max ZSTD compression
    }

    producer = BlockStreamProducer(kafka_config, bitcoin_node, db_url)
    logger.info("Starting block stream")

    block_stream = BlockStream(bitcoin_node, producer, state_manager, terminate_event)
    start_height = state_manager.get_last_block_height() + 1
    block_stream.run(start_height)

    logger.info("Block stream stopped.")