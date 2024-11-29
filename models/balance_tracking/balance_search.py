import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class BalanceSearch:
    def __init__(self, db_url: str = None):
        if db_url is None:
            self.db_url = os.environ.get("DB_CONNECTION_STRING",
                                         f"postgresql://postgres:changeit456$@localhost:5432/miner")
        else:
            self.db_url = db_url
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

    def close(self):
        self.engine.dispose()

    def find_indexed_block_height_ranges(self):
        with self.Session() as session:
            ranges_query = text("""
                WITH block_gaps AS (
                    SELECT
                        block_height,
                        block_height - ROW_NUMBER() OVER (ORDER BY block_height) AS grp
                    FROM
                        blocks
                )
                SELECT
                    MIN(block_height) AS start_block,
                    MAX(block_height) AS end_block
                FROM
                    block_gaps
                GROUP BY
                    grp
                ORDER BY
                    start_block;
            """)
            ranges = session.execute(ranges_query).fetchall()
            return ranges
