import os
from neo4j import GraphDatabase


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
