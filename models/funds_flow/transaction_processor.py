import os
from sqlalchemy import Column, Integer, PrimaryKeyConstraint, create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker
from setup_logger import setup_logger
from confluent_kafka import Producer
import json
from setup_logger import logger_extra_data

logger = setup_logger("GraphIndexer")
Base = declarative_base()


class TransactionProcessorState(Base):
    __tablename__ = 'transaction_processor_state'
    block_height = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('block_height'),
    )


class TransactionProcessorManager:
    def __init__(self, db_url: str = None):
        if db_url is None:
            self.db_url = os.environ.get("DB_CONNECTION_STRING",
                                         f"postgresql://postgres:changeit456$@localhost:5432/miner")
        else:
            self.db_url = db_url
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

        Base.metadata.create_all(self.engine)

    def close(self):
        self.engine.dispose()

    def add_block_height(self, block_height):
        with self.Session() as session:
            upsert_query = text("""INSERT INTO transaction_processor_state (block_height) VALUES (:block_height) ON CONFLICT DO NOTHING""")
            session.execute(upsert_query, {"block_height": block_height})
            session.commit()

    def get_last_block_height(self):
        with self.Session() as session:
            ranges_query = text("""SELECT block_height FROM transaction_processor_state ORDER BY block_height DESC LIMIT 1""")
            result = session.execute(ranges_query).fetchone()
            if result is None:
                return -1
            return result[0]

    def check_if_block_height_is_indexed(self, block_height):
        with self.Session() as session:
            ranges_query = text("""SELECT block_height FROM transaction_processor_state WHERE block_height = :block_height LIMIT 1 """)
            block_height = session.execute(ranges_query, {"block_height": block_height}).scalar_one_or_none()
            return block_height is not None


class TransactionProcessor:
    def __init__(self, kafka_config, bitcoin_node):
        self.producer = Producer(kafka_config)
        self.bitcoin_node = bitcoin_node

    def publish_transaction(self, block_data, batch_size=8):
        transactions = block_data.transactions
        idx = 0
        try:
            for i in range(0, len(transactions), batch_size):
                batch_transactions = transactions[i: i + batch_size]

                # Process all transactions, inputs, and outputs in the current batch
                organized_transactions = []
                for tx in batch_transactions:
                    in_amount_by_address, out_amount_by_address, input_addresses, output_addresses, in_total_amount, out_total_amount = self.bitcoin_node.process_in_memory_txn_for_indexing(tx)

                    inputs = [{"address": address, "amount": in_amount_by_address[address], "tx_id": tx.tx_id} for address in input_addresses]
                    outputs = [{"address": address, "amount": out_amount_by_address[address], "tx_id": tx.tx_id} for address in output_addresses]

                    organized_transactions.append({
                        "tx_id": tx.tx_id,
                        "tx_index": idx,
                        "timestamp": tx.timestamp,
                        "block_height": tx.block_height,
                        "is_coinbase": tx.is_coinbase,
                        "in_total_amount": in_total_amount,
                        "out_total_amount": out_total_amount,
                        "vins": inputs,
                        "vouts": outputs,
                    })
                    idx = idx + 1

                self._send_to_stream("bitcoin-transactions", organized_transactions)

            return True

        except Exception as e:
            logger.error(f"An exception occurred", extra=logger_extra_data(
                error={'exception_type': e.__class__.__name__, 'exception_message': str(e), 'exception_args': e.args}))
            return False

    def _send_to_stream(self, topic, transactions):
        """Helper function to send transactions to a Redpanda stream."""
        try:
            for transaction in transactions:
                self.producer.produce(topic, key=transaction["tx_id"], value=json.dumps(transaction))
            self.producer.flush()
        except Exception as e:
            logger.error(f"Failed to produce transaction: {e}", extra={"transactions": transactions})
