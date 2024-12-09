import argparse
import os
import sys
import signal
import threading
from dotenv import load_dotenv
from loguru import logger
from neo4j import Driver, GraphDatabase
from typing_extensions import LiteralString
from models.block_stream_consumer_base import BlockStreamConsumerBase
from models.block_stream_cursor import BlockStreamCursorManager
from typing import Dict, Any


class BlockStreamConsumer(BlockStreamConsumerBase):
    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 graph_database: Driver,
                 terminate_event,
                 partition):

        super().__init__(kafka_config, block_stream_cursor_manager, terminate_event, partition)

        self.graph_database = graph_database
        self.CONSUMER_NAME = 'money-flow-consumer'

    def index_transaction(self, tx):

        query: LiteralString = """
            MERGE (t:Transaction {tx_id: $tx_id})
            ON CREATE SET 
                t.timestamp = $timestamp,
                t.block_height = $block_height,
                t.is_coinbase = $is_coinbase

            WITH t
            UNWIND $vins as vin
            MERGE (a1:Address {address: vin.address})
            WITH t, vin, a1
            WITH t, collect({n: a1, data: vin}) as inputs

            WITH t, inputs
            UNWIND $vouts as vout
            MERGE (a2:Address {address: vout.address})
            WITH t, inputs, vout, a2
            WITH t, inputs, collect({n: a2, data: vout}) as outputs

            UNWIND inputs as input
            UNWIND outputs as output
            WITH t, input['n'] as from_addr, output['n'] as to_addr, input['data'] as vin_data, output['data'] as vout_data
            MERGE (from_addr)-[:SENT {amount: vin_data.amount}]->(t)
            MERGE (t)-[:SENT {amount: vout_data.amount}]->(to_addr)
            MERGE (from_addr)-[:SENT {
                amount: vout_data.amount,
                block_height: $block_height
            }]->(to_addr)
            """

        with self.graph_database.session() as session:
            try:
                session.run(query, parameters={
                            "tx_id": tx["tx_id"],
                            "timestamp": tx["timestamp"],
                            "block_height": tx["block_height"],
                            "is_coinbase": tx["is_coinbase"],
                            "vins": tx["vins"],
                            "vouts": tx["vouts"]
                        })

            except Exception as e:
                logger.error(f"Error indexing transaction: {e}")
                raise e


if __name__ == "__main__":
    terminate_event = threading.Event()

    def shutdown_handler(signum, frame):
        logger.info(
            "Shutdown signal received. Waiting for current processing to complete."
        )
        terminate_event.set()


    load_dotenv()

    parser = argparse.ArgumentParser(description='Process archive mode settings.')
    parser.add_argument(
        '--archive',
        action='store_true',
        help='Run in archive mode'
    )

    args = parser.parse_args()
    archive = args.archive

    service_name = 'money-flow-archive-consumer' if archive else 'money-flow-live-consumer'

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

    partition_start = 0
    if not archive:
        partition_start = os.getenv("MONEY_FLOW_LIVE_PARTITION_START", 13)

    block_stream_cursor_manager = BlockStreamCursorManager(consumer_name=service_name, db_url=db_url)

    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'group.id': service_name,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'isolation.level': 'read_committed'
    }

    graph_db_url = os.getenv(
        "MONEY_FLOW_MEMGRAPH_ARCHIVE_URL" if archive else "MONEY_FLOW_MEMGRAPH_LIVE_URL",
        "bolt://localhost:7688"
    )

    graph_db_user = os.getenv("MONEY_FLOW_MEMGRAPH_ARCHIVE_USER" if archive else "MONEY_FLOW_MEMGRAPH_LIVE_USER", "memgraph")
    graph_db_password = os.getenv("MONEY_FLOW_MEMGRAPH_ARCHIVE_PASSWORD" if archive else "MONEY_FLOW_MEMGRAPH_LIVE_PASSWORD", "memgraph")

    graph_database = GraphDatabase.driver(
        graph_db_url,
        auth=(graph_db_user, graph_db_password),
        max_connection_lifetime=3600,
        connection_acquisition_timeout=60
    )

    try:
        consumer = BlockStreamConsumer(
            kafka_config,
            block_stream_cursor_manager,
            graph_database,
            terminate_event,
            partition_start
        )
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        block_stream_cursor_manager.close()
        graph_database.close()
        logger.info("Money flow indexer consumer stopped")

