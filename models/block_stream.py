import os
import sys
import signal
import threading
import time
import json
import traceback
from confluent_kafka.admin import AdminClient
from confluent_kafka import admin
from confluent_kafka import Producer
from loguru import logger
from models import BLOCK_STREAM_TOPIC_NAME
from models.block_stream_state import BlockStreamStateManager
from node.node import BitcoinNode
from node.node_utils import parse_block_data


class BlockRangePartitioner:
    def __init__(self, num_partitions: int = 39):
        self.num_partitions = num_partitions
        self.range_size = 52560  # 144 blocks/day * 365 days

        self.partition_ranges = {
            i: (i * self.range_size, (i + 1) * self.range_size - 1)
            for i in range(num_partitions)
        }
        logger.info("Initialized partitioner",
                    partition_ranges=self.partition_ranges,
                    blocks_per_year=self.range_size)

    def __call__(self, block_height):
        partition = block_height // self.range_size
        if partition >= self.num_partitions:
            partition = self.num_partitions - 1
        if block_height % self.range_size == 0:
            logger.info("Reached partition boundary",
                        block_height=block_height,
                        partition=partition,
                        year_number=partition + 1)
        return int(partition)

    def get_partition_range(self, partition):
        start = partition * self.range_size
        end = (partition + 1) * self.range_size - 1
        if partition == self.num_partitions - 1:
            end = float('inf')
        return (start, end)


class BlockStreamProducer:
    def __init__(self, kafka_config, bitcoin_node):
        self.producer = Producer(kafka_config)
        self.bitcoin_node = bitcoin_node

        self.topic_name = BLOCK_STREAM_TOPIC_NAME
        self.partitioner = BlockRangePartitioner()

        self.admin_client = AdminClient(kafka_config)
        self.ensure_topic_exists()

    def ensure_topic_exists(self):
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            if self.topic_name not in metadata.topics:
                new_topic = admin.NewTopic(self.topic_name,
                                           num_partitions=self.num_partitions,
                                           replication_factor=1,
                                           config={'retention.ms': '-1', 'retention.bytes': '-1'})

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

    def process(self, block_data):
        transactions = block_data.transactions

        try:
            organized_transactions = []
            for i in range(0, len(transactions)):
                tx = transactions[i]
                in_amount_by_address, out_amount_by_address, input_addresses, output_addresses, in_total_amount, out_total_amount = self.bitcoin_node.process_in_memory_txn_for_indexing(tx)

                inputs = [{"address": address, "amount": in_amount_by_address[address], "tx_id": tx.tx_id} for address in input_addresses]
                outputs = [{"address": address, "amount": out_amount_by_address[address], "tx_id": tx.tx_id} for address in output_addresses]

                organized_transactions.append({
                    "tx_id": tx.tx_id,
                    "tx_index": i,
                    "timestamp": tx.timestamp,
                    "block_height": tx.block_height,
                    "is_coinbase": tx.is_coinbase,
                    "in_total_amount": in_total_amount,
                    "out_total_amount": out_total_amount,
                    "vins": inputs,
                    "vouts": outputs,
                    "size": tx.size,
                    "vsize": tx.vsize,
                    "weight": tx.weight
                })

            self._send_to_stream(self.topic_name, organized_transactions)

            return True

        except Exception as e:
            logger.error(f"An exception occurred",  error=e, trb=traceback.format_exc())
            return False

    def _send_to_stream(self, topic, transactions):
        try:
            for transaction in transactions:

                partition = self.partitioner(transaction["block_height"])

                self.producer.produce(topic,
                                      key=transaction["tx_id"],
                                      value=json.dumps(transaction),
                                      partition=partition)
            self.producer.flush()
        except Exception as e:
            logger.error(f"An exception occurred",  error=e, trb=traceback.format_exc())


class BlockStream:

    def __init__(self,
                 bitcoin_node,
                 block_stream_producer,
                 block_stream_state_manager,
                 terminate_event: threading.Event):

        self.bitcoin_node = bitcoin_node
        self.block_stream_producer = block_stream_producer
        self.block_stream_state_manager = block_stream_state_manager
        self.terminate_event = terminate_event

    def index_block(self, block_height):
        try:
            block = self.bitcoin_node.get_block_by_height(block_height)
            num_transactions = len(block["tx"])
            start_time = time.time()
            block_data = parse_block_data(block)

            success = self.block_stream_producer.process(block_data)

            end_time = time.time()
            time_taken = end_time - start_time
            formatted_num_transactions = "{:>4}".format(num_transactions)
            formatted_time_taken = "{:6.2f}".format(time_taken)
            formatted_tps = "{:8.2f}".format(
                num_transactions / time_taken if time_taken > 0 else float("inf")
            )

            if time_taken > 0:
                logger.info("Streaming transactions", block_height=f"{block_height:>6}", num_transactions=formatted_num_transactions, time_taken=formatted_time_taken, tps=formatted_tps)
            else:
                logger.info("Streamed transactions in 0.00 seconds (  Inf TPS).", block_height=f"{block_height:>6}", num_transactions=formatted_num_transactions)

            return success
        except Exception as e:
            logger.error(f"Failed to index block.", block_height=block_height, error=str(e))
            return False

    def run(self, start_height: int):
        skip_blocks = 6
        forward_block_height = start_height

        while not self.terminate_event.is_set():
            current_block_height = self.bitcoin_node.get_current_block_height() - skip_blocks
            block_height = forward_block_height

            if block_height > current_block_height:
                logger.info(
                    f"Waiting for new blocks.",
                    block_height=current_block_height
                )
                time.sleep(10)
                continue

            while self.block_stream_state_manager.check_if_block_height_is_indexed(block_height):
                logger.info(f"Skipping block. Already indexed.", block_height=block_height)
                block_height += 1

            success = self.index_block(block_height)

            if success:
                self.block_stream_state_manager.add_block_height(block_height)
                forward_block_height = block_height + 1
            else:
                logger.error(f"Failed to index block.", block_height=block_height)
                time.sleep(30)


terminate_event = threading.Event()


def shutdown_handler(signum, frame):
    logger.info(
        "Shutdown signal received. Waiting for current processing to complete before shutting down."
    )
    terminate_event.set()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    def patch_record(record):
        record["extra"]["service"] = 'bitcoin-block-stream'
        return True

    logger.remove()
    logger.add(
        "../../logs/bitcoin-block-stream.log",
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

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    db_url = os.getenv("BLOCK_STREAM_DB_CONNECTION_STRING", "postgresql://postgres:changeit456$@localhost:5420/block_stream")
    redpanda_bootstrap_servers = os.getenv("BLOCK_STREAM_REDPANDA_BOOTSTRAP_SERVERS", "localhost:19092")
    bitcoin_node_rpc_url = os.getenv("BITCOIN_NODE_RPC_URL", "http://bitcoin-node:8332")

    bitcoin_node = BitcoinNode(bitcoin_node_rpc_url)
    block_stream_state_manager = BlockStreamStateManager(db_url)
    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'enable.idempotence': True,
    }
    block_stream_producer = BlockStreamProducer(kafka_config, bitcoin_node)
    logger.info("Starting block stream")

    block_stream_producer = BlockStream(bitcoin_node, block_stream_producer, block_stream_state_manager, terminate_event)
    start_height = block_stream_state_manager.get_last_block_height() + 1
    block_stream_producer.run(start_height)

    logger.info("Block stream stopped.")
