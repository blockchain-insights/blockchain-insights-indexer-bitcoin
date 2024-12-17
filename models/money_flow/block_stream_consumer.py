import argparse
import os
import sys
import signal
import threading
from dotenv import load_dotenv
from loguru import logger
from neo4j import Driver, GraphDatabase
from typing_extensions import LiteralString
from models.block_stream_consumer_base import BlockStreamConsumerBase, PartitionBasedConsumer, LiveBlockStreamConsumer
from models.block_stream_cursor import BlockStreamCursorManager
from typing import Dict, Any, List


class MoneyFlowIndexer:
    def __init__(self, graph_database: Driver):
        self.graph_database = graph_database

    def index_transactions_in_batches(self, transactions):
        with self.graph_database.session() as session:
            for tx in transactions:
                self.index_transaction(session, tx)

    def index_transaction(self, session, tx):
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


class MoneyFlowArchiveConsumer(PartitionBasedConsumer):
    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 money_flow_indexer: MoneyFlowIndexer,
                 terminate_event,
                 partition: int,
                 batch_size: int = 1000):

        super().__init__(
            kafka_config=kafka_config,
            block_stream_cursor_manager=block_stream_cursor_manager,
            terminate_event=terminate_event,
            consumer_name='money-flow-consumer',
            partition=partition,
            batch_size=batch_size
        )

        self.money_flow_indexer = money_flow_indexer

    def process_transactions(self, transactions: List[Dict]):
        self.money_flow_indexer.index_transactions_in_batches(transactions)


class MoneyFlowLiveConsumer(LiveBlockStreamConsumer):
    def __init__(self,
                 kafka_config: Dict[str, Any],
                 block_stream_cursor_manager: BlockStreamCursorManager,
                 money_flow_indexer: MoneyFlowIndexer,
                 terminate_event,
                 batch_size: int = 1000):

        super().__init__(
            kafka_config=kafka_config,
            block_stream_cursor_manager=block_stream_cursor_manager,
            terminate_event=terminate_event,
            consumer_name='money-flow-consumer',
            batch_size=batch_size
        )

        self.money_flow_indexer = money_flow_indexer

    def process_transactions(self, transactions: List[Dict]):
        self.money_flow_indexer.index_transactions_in_batches(transactions)


if __name__ == "__main__":
    terminate_event = threading.Event()

    def shutdown_handler(signum, frame):
        logger.info("Shutdown signal received. Waiting for current processing to complete.")
        terminate_event.set()

    load_dotenv()

    parser = argparse.ArgumentParser(description='Bitcoin Block Stream Consumer')
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--money-flow-archive', action='store_true', help='Run in archive mode')
    mode_group.add_argument('--money-flow-live', action='store_true', help='Run in live mode')
    parser.add_argument('--partition', type=int, help='Partition number to process (required for archive mode)')
    args = parser.parse_args()

    is_live_mode = args.money_flow_live

    # Validate arguments
    if not is_live_mode and args.partition is None:
        parser.error("--partition is required when using --money-flow-archive")

    base_name = 'money-flow-live' if is_live_mode else 'money-flow-archive'
    service_name = f'{base_name}-partition-{args.partition}-consumer' if args.partition is not None else f'{base_name}-consumer'

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

    # Initialize components
    db_url = os.getenv(
        "REDPANDA_DB_CONNECTION_STRING",
        "postgresql://postgres:changeit456$@localhost:5420/block_stream"
    )
    redpanda_bootstrap_servers = os.getenv(
        "REDPANDA_BOOTSTRAP_SERVERS",
        "localhost:19092"
    )

    block_stream_cursor_manager = BlockStreamCursorManager(consumer_name=service_name, db_url=db_url)

    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'group.id': service_name,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.partition.fetch.bytes': 134217728,
        'fetch.max.bytes': 134217728,
        'receive.message.max.bytes': 134218240,  # + 512 bytes
    }

    graph_db_url = os.getenv(
        "MONEY_FLOW_MEMGRAPH_ARCHIVE_URL" if not is_live_mode else "MONEY_FLOW_MEMGRAPH_LIVE_URL",
        "bolt://localhost:7688"
    )

    graph_db_user = os.getenv(
        "MONEY_FLOW_MEMGRAPH_ARCHIVE_USER" if not is_live_mode else "MONEY_FLOW_MEMGRAPH_LIVE_USER",
        "memgraph"
    )
    graph_db_password = os.getenv(
        "MONEY_FLOW_MEMGRAPH_ARCHIVE_PASSWORD" if not is_live_mode else "MONEY_FLOW_MEMGRAPH_LIVE_PASSWORD",
        "memgraph"
    )

    graph_database = GraphDatabase.driver(
        graph_db_url,
        auth=(graph_db_user, graph_db_password),
        max_connection_lifetime=3600,
        connection_acquisition_timeout=60
    )

    money_flow_indexer = MoneyFlowIndexer(graph_database)

    try:
        if is_live_mode:
            consumer = MoneyFlowLiveConsumer(
                kafka_config,
                block_stream_cursor_manager,
                money_flow_indexer,
                terminate_event,
                1000
            )
            consumer.run()
        else:
            consumer = MoneyFlowArchiveConsumer(
                kafka_config,
                block_stream_cursor_manager,
                money_flow_indexer,
                terminate_event,
                args.partition,
                1000
            )
            consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        block_stream_cursor_manager.close()
        graph_database.close()
        logger.info("Indexer stopped")