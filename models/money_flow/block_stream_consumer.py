import json
import os
import sys
import signal
import threading
from sre_constants import error

import mgp
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
                 terminate_event):

        super().__init__(kafka_config, block_stream_cursor_manager, terminate_event)

        self.graph_database = graph_database
        self.CONSUMER_NAME = 'money-flow-consumer'

    def index_transaction(self, tx):
        with self.graph_database.session() as session:
            try:
                with session.begin_transaction() as tx_context:
                    tx_context.run(
                        query="""
                         MERGE (t:Transaction {tx_id: $tx_id})
                         ON CREATE SET t.timestamp = $timestamp,
                                       t.block_height = $block_height,
                                       t.is_coinbase = $is_coinbase,
                                       t.in_total_amount = $in_total_amount,
                                       t.out_total_amount = $out_total_amount
                         """,
                        parameters={
                            "tx_id": tx["tx_id"],
                            "timestamp": tx["timestamp"],
                            "block_height": tx["block_height"],
                            "is_coinbase": tx["is_coinbase"],
                            "in_total_amount": tx["in_total_amount"],
                            "out_total_amount": tx["out_total_amount"]
                        }
                    )
                    # Create Address nodes and relationships for inputs (vins)
                    for vin in tx["vins"]:
                        # Create source Address node
                        tx_context.run(
                            query="MERGE (a:Address {address: $address})",
                            parameters={"address": vin["address"]}
                        )
                        # Create relationship from Address to Transaction
                        tx_context.run(
                            query="""
                            MATCH (a:Address {address: $address})
                            MATCH (t:Transaction {tx_id: $tx_id})
                            MERGE (a)-[r:SENT {
                                amount: $amount
                            }]->(t)
                            """,
                            parameters={
                                "address": vin["address"],
                                "tx_id": tx["tx_id"],
                                "amount": vin["amount"]
                            }
                        )
                    # Create Address nodes and relationships for outputs (vouts)
                    for vout in tx["vouts"]:
                        # Create destination Address node
                        tx_context.run(
                            query="MERGE (a:Address {address: $address})",
                            parameters={"address": vout["address"]}
                        )
                        # Create relationship from Transaction to Address
                        tx_context.run(
                            query="""
                            MATCH (t:Transaction {tx_id: $tx_id})
                            MATCH (a:Address {address: $address})
                            MERGE (t)-[r:SENT {
                                amount: $amount
                            }]->(a)
                            """,
                            parameters={
                                "tx_id": tx["tx_id"],
                                "address": vout["address"],
                                "amount": vout["amount"]
                            }
                        )
                    # Create direct Address-to-Address relationships
                    for vin in tx["vins"]:
                        for vout in tx["vouts"]:
                            tx_context.run(
                                query="""
                                MATCH (from:Address {address: $from_address})
                                MATCH (to:Address {address: $to_address})
                                MERGE (from)-[r:SENT {
                                    amount: $amount,
                                    block_height: $block_height
                                }]->(to)
                                """,
                                parameters={
                                    "from_address": vin["address"],
                                    "to_address": vout["address"],
                                    "amount": vout["amount"],
                                    "block_height": tx["block_height"]
                                }
                            )
                        tx_context.commit()
            except Exception as e:
                tx_context.rollback()
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

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


    def patch_record(record):
        record["extra"]["service"] = 'money-flow-consumer'
        return True

    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <blue>{message}</blue> | {extra}",
        level="DEBUG",
        filter=patch_record,
    )

    logger.add(
        "logs/money-flow-consumer.log",
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

    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'group.id': 'money-flow-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'isolation.level': 'read_committed'
    }

    graph_db_url = os.getenv(
        "MONEY_FLOW_MEMGRAPH_ARCHIVE_URL",
        "bolt://localhost:7688"
    )

    graph_db_user = os.getenv("MONEY_FLOW_MEMGRAPH_ARCHIVE_USER", "neo4j")
    graph_db_password = os.getenv("MONEY_FLOW_MEMGRAPH_ARCHIVE_PASSWORD", "neo4j")

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
            terminate_event
        )
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        block_stream_cursor_manager.close()
        logger.info("Balance indexer consumer stopped")
