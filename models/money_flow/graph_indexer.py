import os
from setup_logger import setup_logger
from setup_logger import logger_extra_data
from neo4j import GraphDatabase

logger = setup_logger("GraphIndexer")


class GraphIndexer:
    def __init__(
        self,
        graph_db_url: str = None,
        graph_db_user: str = None,
        graph_db_password: str = None,
    ):
        if graph_db_url is None:
            self.graph_db_url = (
                os.environ.get("GRAPH_DB_URL") or "bolt://localhost:7687"
            )
        else:
            self.graph_db_url = graph_db_url

        if graph_db_user is None:
            self.graph_db_user = os.environ.get("GRAPH_DB_USER") or ""
        else:
            self.graph_db_user = graph_db_user

        if graph_db_password is None:
            self.graph_db_password = os.environ.get("GRAPH_DB_PASSWORD") or ""
        else:
            self.graph_db_password = graph_db_password

        self.driver = GraphDatabase.driver(
            self.graph_db_url,
            auth=(self.graph_db_user, self.graph_db_password),
        )

    def close(self):
        self.driver.close()

    def check_database_type(self):
        with self.driver.session() as session:
            try:
                # Try Neo4j-specific query first
                result = session.run("CALL dbms.components() YIELD name, versions, edition")
                print("result {}".format(result))
                record = result.single()
                print("record {}".format(record))

                if record and "Neo4j" in record["name"]:
                    version = record["versions"][0]
                    edition = record["edition"]
                    return f"Neo4j {edition} {version}"
                if record and "Memgraph" in record["name"]:
                    version = record["versions"][0]
                    edition = record["edition"]
                    return f"Memgraph {edition} {version}"
            except Exception as e:
                print(f"exception: {e}")
                return "Unknown graph database"


    def set_min_max_block_height_cache(self, min_block_height, max_block_height):
        with self.driver.session() as session:
            # update min block height
            session.run(
                """
                MERGE (n:Cache {field: 'min_block_height'})
                SET n.value = $min_block_height
                RETURN n
                """,
                {"min_block_height": min_block_height}
            )

            # update max block height
            session.run(
                """
                MERGE (n:Cache {field: 'max_block_height'})
                SET n.value = $max_block_height
                RETURN n
                """,
                {"max_block_height": max_block_height}
            )

    def check_if_block_is_indexed(self, block_height: int) -> bool:
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (t: Transaction{block_height: $block_height})
                RETURN t
                LIMIT 1;
                """,
                block_height=block_height
            )
            single_result = result.single()
            return single_result is not None

    def find_indexed_block_height_ranges(self):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (t:Transaction)
                RETURN DISTINCT t.block_height AS block_height
                ORDER BY block_height
                """,
            )
            block_heights = [record["block_height"] for record in result]

            if not block_heights:
                return []

            # Group consecutive gaps into ranges
            gap_ranges = []
            current_start = block_heights[0]
            current_end = block_heights[0]

            for height in block_heights[1:]:
                if height == current_end + 1:
                    # Consecutive gap, extend the current range
                    current_end = height
                else:
                    # Non-consecutive gap, start a new range
                    gap_ranges.append((current_start, current_end))
                    current_start = height
                    current_end = height

            # Add the last range
            gap_ranges.append((current_start, current_end))

            return gap_ranges

    def create_neo4j_indexes(self):
        with self.driver.session() as session:
            # Fetch existing indexes and constraints
            existing_indexes = session.run("SHOW INDEXES")
            existing_constraints = session.run("SHOW CONSTRAINTS")
            existing_index_set = set()
            existing_constraint_set = set()

            for record in existing_indexes:
                label = record["labelsOrTypes"][0] if record["labelsOrTypes"] else None
                properties = record["properties"]
                index_name = f"{label}-{properties[0]}" if properties else label
                if index_name:
                    existing_index_set.add(index_name)

            for record in existing_constraints:
                label = record["labelsOrTypes"][0] if record["labelsOrTypes"] else None
                properties = record["properties"]
                constraint_name = f"{label}-{properties[0]}" if properties else label
                if constraint_name:
                    existing_constraint_set.add(constraint_name)

            index_creation_statements = {
                "Cache-cache_id": "CREATE INDEX FOR (n:Cache) ON (n.cache_id)",
                "Transaction-tx_id": "CREATE INDEX FOR (n:Transaction) ON (n.tx_id)",
                "Transaction-block_height": "CREATE INDEX FOR (n:Transaction) ON (n.block_height)",
                "Transaction-out_total_amount": "CREATE INDEX FOR (n:Transaction) ON (n.out_total_amount)",
                "Address-address": "CREATE INDEX FOR (n:Address) ON (n.address)",
                "SENT-value_satoshi": "CREATE INDEX FOR ()-[r:SENT]-() ON (r.value_satoshi)"
            }

            constraint_statements = {
                "Transaction-tx_id": "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_id IS UNIQUE",
                "Address-address": "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE"
            }

            # Create indexes (if not exist and no corresponding constraint exists)
            for index_name, statement in index_creation_statements.items():
                if index_name not in existing_index_set and index_name not in existing_constraint_set:
                    try:
                        logger.info("Creating index", extra=logger_extra_data(index_name=index_name))
                        session.run(statement)
                    except Exception as e:
                        logger.error(
                            "An exception occurred while creating index",
                            extra=logger_extra_data(
                                index_name=index_name,
                                error={
                                    'exception_type': e.__class__.__name__,
                                    'exception_message': str(e),
                                    'exception_args': e.args
                                }
                            )
                        )

            # Create constraints (if not exist, dropping existing index if necessary)
            for constraint_name, statement in constraint_statements.items():
                if constraint_name not in existing_constraint_set:
                    try:
                        # If an index exists, drop it first
                        if constraint_name in existing_index_set:
                            drop_index_statement = f"DROP INDEX ON :{constraint_name.split('-')[0]}({constraint_name.split('-')[1]})"
                            logger.info("Dropping existing index before creating constraint",
                                        extra=logger_extra_data(index_name=constraint_name))
                            session.run(drop_index_statement)

                        logger.info("Creating constraint", extra=logger_extra_data(constraint=statement))
                        session.run(statement)
                    except Exception as e:
                        logger.error(
                            "An exception occurred while creating constraint",
                            extra=logger_extra_data(
                                constraint=statement,
                                error={
                                    'exception_type': e.__class__.__name__,
                                    'exception_message': str(e),
                                    'exception_args': e.args
                                }
                            )
                        )

            logger.info("Syncing block range caches...")

    from decimal import getcontext

    # Set the precision high enough to handle satoshis for Bitcoin transactions
    getcontext().prec = 28

    def create_memgraph_indexes(self):
        with self.driver.session() as session:
            # Fetch existing indexes
            existing_indexes = session.run("SHOW INDEX INFO")
            existing_index_set = set()
            for record in existing_indexes:
                label = record["label"]
                property = record["property"]
                index_name = f"{label}-{property}" if property else label
                if index_name:
                    existing_index_set.add(index_name)

            index_creation_statements = {
                "Cache": "CREATE INDEX ON :Cache;",
                "Transaction": "CREATE INDEX ON :Transaction;",
                "Transaction-tx_id": "CREATE INDEX ON :Transaction(tx_id);",
                "Transaction-block_height": "CREATE INDEX ON :Transaction(block_height);",
                "Transaction-out_total_amount": "CREATE INDEX ON :Transaction(out_total_amount)",
                "Address-address": "CREATE INDEX ON :Address(address);",
                "SENT-value_satoshi": "CREATE INDEX ON :SENT(value_satoshi)",
            }

            # Add constraints (Note: Memgraph uses a different syntax for constraints)
            constraint_statements = [
                "CREATE CONSTRAINT ON (t:Transaction) ASSERT t.tx_id IS UNIQUE",
                "CREATE CONSTRAINT ON (a:Address) ASSERT a.address IS UNIQUE"
            ]

            # Create indexes
            for index_name, statement in index_creation_statements.items():
                if index_name not in existing_index_set:
                    try:
                        logger.info(f"Creating index", extra=logger_extra_data(index_name=index_name))
                        session.run(statement)
                    except Exception as e:
                        logger.error(f"An exception occurred while creating index",
                                     extra=logger_extra_data(index_name=index_name,
                                                             error={'exception_type': e.__class__.__name__,
                                                                    'exception_message': str(e),
                                                                    'exception_args': e.args}))

            # Create constraints
            for constraint in constraint_statements:
                try:
                    logger.info(f"Creating constraint", extra=logger_extra_data(constraint=constraint))
                    session.run(constraint)
                except Exception as e:
                    logger.error(f"An exception occurred while creating constraint",
                                 extra=logger_extra_data(constraint=constraint,
                                                         error={'exception_type': e.__class__.__name__,
                                                                'exception_message': str(e),
                                                                'exception_args': e.args}))

    def create_indexes(self):
        db_type = self.check_database_type()
        print("db_type", db_type)
        if "Neo4j" in db_type:
            self.create_neo4j_indexes()
        elif "Memgraph" in db_type:
            self.create_memgraph_indexes()
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def create_graph_focused_on_money_flow(self, block_data, _bitcoin_node, batch_size=8):
        transactions = block_data.transactions

        with self.driver.session() as session:
            # Start a transaction
            transaction = session.begin_transaction()

            try:
                for i in range(0, len(transactions), batch_size):
                    batch_transactions = transactions[i : i + batch_size]

                    # Process all transactions, inputs, and outputs in the current batch
                    batch_txns = []
                    batch_inputs = []
                    batch_outputs = []
                    for tx in batch_transactions:
                        in_amount_by_address, out_amount_by_address, input_addresses, output_addresses, in_total_amount, out_total_amount = _bitcoin_node.process_in_memory_txn_for_indexing(tx)
                        
                        inputs = [{"address": address, "amount": in_amount_by_address[address], "tx_id": tx.tx_id } for address in input_addresses]
                        outputs = [{"address": address, "amount": out_amount_by_address[address], "tx_id": tx.tx_id } for address in output_addresses]

                        batch_txns.append({
                            "tx_id": tx.tx_id,
                            "in_total_amount": in_total_amount,
                            "out_total_amount": out_total_amount,
                            "timestamp": tx.timestamp,
                            "block_height": tx.block_height,
                            "is_coinbase": tx.is_coinbase,
                        })
                        batch_inputs += inputs
                        batch_outputs += outputs

                    transaction.run(
                        """
                        UNWIND $transactions AS tx
                        MERGE (t:Transaction {tx_id: tx.tx_id})
                        ON CREATE SET t.timestamp = tx.timestamp,
                                    t.in_total_amount = tx.in_total_amount,
                                    t.out_total_amount = tx.out_total_amount,
                                    t.timestamp = tx.timestamp,
                                    t.block_height = tx.block_height,
                                    t.is_coinbase = tx.is_coinbase
                        """,
                        transactions=batch_txns,
                    )
                    
                    transaction.run(
                        """
                        UNWIND $inputs AS input
                        MERGE (a:Address {address: input.address})
                        MERGE (t:Transaction {tx_id: input.tx_id})
                        CREATE (a)-[:SENT { value_satoshi: input.amount }]->(t)
                        """,
                        inputs=batch_inputs
                    )
                    
                    transaction.run(
                        """
                        UNWIND $outputs AS output
                        MERGE (a:Address {address: output.address})
                        MERGE (t:Transaction {tx_id: output.tx_id})
                        CREATE (t)-[:SENT { value_satoshi: output.amount }]->(a)
                        """,
                        outputs=batch_outputs
                    )

                transaction.commit()
                return True

            except Exception as e:
                transaction.rollback()
                logger.error(f"An exception occurred", extra = logger_extra_data(error = {'exception_type': e.__class__.__name__,'exception_message': str(e),'exception_args': e.args}))
                return False

            finally:
                if transaction.closed() is False:
                    transaction.close()
