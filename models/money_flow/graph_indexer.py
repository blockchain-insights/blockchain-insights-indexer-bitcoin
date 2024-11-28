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

    from decimal import getcontext

    # Set the precision high enough to handle satoshis for Bitcoin transactions
    getcontext().prec = 28
