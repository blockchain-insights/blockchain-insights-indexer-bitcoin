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


    def get_cursor(self, partition: Optional[int]) -> Optional[BlockStreamCursor]:
        """
        Get cursor for specified consumer and partition.
        Returns None if no cursor exists.
        """
        try:
            with self.Session() as session:
                if partition is None:
                    cursor = session.query(BlockStreamCursor).filter(
                        BlockStreamCursor.consumer_name == self.consumer_name
                    ).order_by(BlockStreamCursor.partition.desc()).first()
                    return cursor

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
