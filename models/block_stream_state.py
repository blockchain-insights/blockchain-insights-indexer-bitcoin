from sqlalchemy import Column, Integer, PrimaryKeyConstraint, create_engine, text, String, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from models import BLOCK_STREAM_TOPIC_NAME

Base = declarative_base()


class BlockStreamState(Base):

    __tablename__ = 'block_stream_state'

    block_height = Column(Integer, nullable=False)
    topic = Column(String, nullable=False)
    network = Column(String, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('block_height', 'topic', 'network'),
        UniqueConstraint('block_height', 'topic', network),
    )


class BlockStreamStateManager:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

        Base.metadata.create_all(self.engine)

    def close(self):
        self.engine.dispose()

    def add_block_height(self, block_height, topic=BLOCK_STREAM_TOPIC_NAME, network="bitcoin"):
        with self.Session() as session:
            upsert_query = text("""INSERT INTO block_stream_state (block_height, topic, network) VALUES (:block_height, :topic, :network) ON CONFLICT DO NOTHING""")
            session.execute(upsert_query, {"block_height": block_height, "topic": topic, "network": network})
            session.commit()

    def get_last_block_height(self, topic=BLOCK_STREAM_TOPIC_NAME, network="bitcoin"):
        with self.Session() as session:
            ranges_query = text("""SELECT block_height FROM block_stream_state WHERE topic = :topic AND network = :network ORDER BY block_height DESC LIMIT 1""")
            result = session.execute(ranges_query, {"topic": topic, "network": network}).fetchone()
            if result is None:
                return -1
            return result[0]

    def check_if_block_height_is_indexed(self, block_height, topic=BLOCK_STREAM_TOPIC_NAME, network="bitcoin"):
        with self.Session() as session:
            ranges_query = text("""SELECT block_height FROM block_stream_state WHERE block_height = :block_height AND topic = :topic AND network = :network""")
            block_height = session.execute(ranges_query, {"block_height": block_height, "topic": topic, "network": network}).fetchone()
            return block_height is not None