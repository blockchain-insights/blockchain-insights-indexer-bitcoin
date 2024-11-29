from sqlalchemy import Column, String, Integer, PrimaryKeyConstraint, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class BlockStreamCursor(Base):
    __tablename__ = 'block_stream_cursor'
    consumer_name = Column(String, nullable=False)
    partition = Column(Integer, nullable=False)
    offset = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('consumer_name'),
    )


class BlockStreamCursorManager:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

        Base.metadata.create_all(self.engine)

    def close(self):
        self.engine.dispose()

    def get_cursor(self, consumer_name, partition):
        with self.Session() as session:
            cursor = session.query(BlockStreamCursor).filter(BlockStreamCursor.consumer_name == consumer_name, BlockStreamCursor.partition == partition).first()
            return cursor

    def set_cursor(self, consumer_name, partition, offset, timestamp):
        with self.Session() as session:
            session.execute(
                """
                INSERT INTO consumer_cursor (consumer_name, partition, offset, timestamp)
                VALUES (:consumer_name, :partition, :offset, :timestamp)
                ON CONFLICT (consumer_name) DO UPDATE SET
                    partition = EXCLUDED.partition,
                    offset = EXCLUDED.offset,
                    timestamp = EXCLUDED.timestamp
                """,
                {
                    'consumer_name': consumer_name,
                    'partition': partition,
                    'offset': offset,
                    'timestamp': timestamp
                }
            )
            session.commit()

