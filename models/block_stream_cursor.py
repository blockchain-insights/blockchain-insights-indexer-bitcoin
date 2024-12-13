from datetime import datetime
from typing import Optional
from loguru import logger
from sqlalchemy import Column, String, Integer, PrimaryKeyConstraint, DateTime, create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class BlockStreamCursor(Base):
    __tablename__ = 'block_stream_cursor'
    consumer_name = Column(String, nullable=False)
    partition = Column(Integer, nullable=False)
    offset = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('consumer_name', 'partition'),
    )


class BlockStreamCursorManager:
    def __init__(self, consumer_name: str, db_url: str):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.consumer_name = consumer_name

        # Ensure tables exist
        Base.metadata.create_all(self.engine)

    def close(self):
        self.engine.dispose()

    def find_first_gap(self, start_height: int, end_height: int = None, topic="transactions", network="bitcoin") -> int:
        """Find the first non-indexed block height in the given range"""
        with self.Session() as session:
            # Build query for blocks in range
            query = f"""
                WITH seq AS (
                    SELECT generate_series({start_height}, 
                                        CASE WHEN {end_height} IS NULL 
                                        THEN (SELECT MAX(block_height) FROM block_stream_state WHERE topic = :topic AND network = :network)
                                        ELSE {end_height} END) AS block_height
                )
                SELECT seq.block_height
                FROM seq
                LEFT JOIN block_stream_state bss ON 
                    seq.block_height = bss.block_height AND
                    bss.topic = :topic AND
                    bss.network = :network
                WHERE bss.block_height IS NULL
                ORDER BY seq.block_height
                LIMIT 1
            """
            
            result = session.execute(text(query), 
                                   {'topic': topic, 'network': network}).first()
            
            if result:
                return result[0]
            return None

    def get_cursor(self, partition: int) -> Optional[BlockStreamCursor]:
        """
        Get cursor for specified consumer and partition.
        Returns None if no cursor exists.
        """
        try:
            with self.Session() as session:
                cursor = session.query(BlockStreamCursor).filter(
                    BlockStreamCursor.consumer_name == self.consumer_name,
                    BlockStreamCursor.partition == partition
                ).first()
                return cursor
        except Exception as e:
            logger.error(
                "Failed to get cursor",
                extra={
                    "consumer_name": self.consumer_name,
                    "partition": partition,
                    "error": str(e)
                }
            )
            raise

    def set_cursor(self, partition: int, offset: int, timestamp: datetime) -> None:
        """Set cursor position for consumer."""
        try:
            with self.Session() as session:
                try:
                    session.execute(
                        text("""
                            WITH upsert AS (
                                UPDATE block_stream_cursor SET
                                    "offset" = :offset,
                                    timestamp = :timestamp
                                WHERE consumer_name = :consumer_name 
                                AND partition = :partition
                                RETURNING *
                            )
                            INSERT INTO block_stream_cursor (consumer_name, partition, "offset", timestamp)
                            SELECT :consumer_name, :partition, :offset, :timestamp
                            WHERE NOT EXISTS (SELECT * FROM upsert)
                        """),
                        {
                            'consumer_name': self.consumer_name,
                            'partition': partition,
                            'offset': offset,
                            'timestamp': timestamp
                        }
                    )
                    session.commit()
                except Exception as e:
                    session.rollback()
                    raise e
        except Exception as e:
            logger.error("Failed to set cursor", error=str(e))
            raise e
