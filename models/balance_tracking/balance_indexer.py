import os
from datetime import datetime

from setup_logger import setup_logger
from setup_logger import logger_extra_data

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import select

from .balance_model import Base, BalanceChange, Block

logger = setup_logger("BalanceIndexer")


class BalanceIndexer:
    def __init__(self, db_url: str = None):
        if db_url is None:
            self.db_url = os.environ.get("DB_CONNECTION_STRING",
                                         f"postgresql://postgres:changeit456$@localhost:5432/miner")
        else:
            self.db_url = db_url

        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

        # check if table exists and create if not
        connection = self.engine.connect()

        # Create an inspector
        inspector = inspect(self.engine)

        # Check if the table already exists
        if (not inspector.has_table('balance_changes')) or (not inspector.has_table('blocks')):
            # Create the table in the database
            Base.metadata.create_all(self.engine)
            logger.info("Created 3 tables: `balance_changes`, `blocks`")

        # Close the connection
        connection.close()

    def close(self):
        self.engine.dispose()

    def _ensure_hypertable_exists(self):
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'balance_changes';"
            )).fetchone()
            if not result:
                conn.execute(text(
                    "SELECT create_hypertable('balance_changes', 'block', chunk_time_interval => 12960, migrate_data => true);"
                ))

    def setup_db(self):
        with self.engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            try:
                # Check if TimescaleDB extension is already installed
                result = conn.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';"))
                if not result.fetchone():
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                    logger.info("TimescaleDB extension created successfully.")
                else:
                    logger.info("TimescaleDB extension already exists.")

                # Check if hypertable is enabled
                result = conn.execute(text(
                    "SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = 'balance_changes';"
                )).fetchone()

                if not result:
                    conn.execute(text(
                        "SELECT create_hypertable('balance_changes', 'block', chunk_time_interval => 12960, migrate_data => true);"
                    ))
                    logger.info("Hypertable 'balance_changes' created successfully.")
                else:
                    logger.info("Hypertable 'balance_changes' already exists.")
            except SQLAlchemyError as e:
                logger.error(f"An error occurred: {e}")

            # Check if the index 'balance_changes_block_idx' exists
            index_check = conn.execute(text(
                "SELECT * FROM pg_indexes WHERE indexname = 'balance_changes_block_idx';"
            )).fetchone()

            if index_check:
                # Index exists, check if it is created on block field with DESC order
                if 'DESC' in index_check[4]:  # The definition of the index is in the 6th column
                    # Drop the existing index
                    conn.execute(text("DROP INDEX IF EXISTS balance_changes_block_idx;"))
                    logger.info("Dropped existing index 'balance_changes_block_idx'.")

                    # Create a new index
                    conn.execute(text("CREATE INDEX balance_changes_block_idx ON public.balance_changes USING btree (block);"))
                    logger.info("Created index 'balance_changes_block_idx' on 'block' field.")

            else:
                # Create index if it doesn't exist
                conn.execute(text("CREATE INDEX balance_changes_block_idx ON public.balance_changes USING btree (block);"))
                logger.info("Created index 'balance_changes_block_idx' on 'block' field.")
                
            # Create indexes if they do not exist
            conn.execute(text(
                "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_timestamp') THEN CREATE INDEX idx_timestamp ON blocks (timestamp); END IF; END $$;"))
            conn.execute(text(
                "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'idx_block_timestamp') THEN CREATE INDEX idx_block_timestamp ON balance_changes (block_timestamp); END IF; END $$;"))

    def get_latest_block_number(self):
        with self.Session() as session:
            try:
                latest_balance_change = session.query(BalanceChange).order_by(BalanceChange.block.desc()).first()
                latest_block = latest_balance_change.block
            except Exception as e:
                latest_block = 0
            return latest_block

    from decimal import getcontext

    # Set the precision high enough to handle satoshis for Bitcoin transactions
    getcontext().prec = 28

    def create_rows_focused_on_balance_changes(self, block_data, _bitcoin_node):
        block_height = block_data.block_height
        block_timestamp = datetime.utcfromtimestamp(block_data.timestamp)
        transactions = block_data.transactions

        balance_changes_by_address = {}
        changed_addresses = []

        for tx in transactions:
            in_amount_by_address, out_amount_by_address, input_addresses, output_addresses, in_total_amount, _ = _bitcoin_node.process_in_memory_txn_for_indexing(
                tx)

            for address in input_addresses:
                if address not in balance_changes_by_address:
                    balance_changes_by_address[address] = 0
                    changed_addresses.append(address)
                balance_changes_by_address[address] -= in_amount_by_address[address]

            for address in output_addresses:
                if address not in balance_changes_by_address:
                    balance_changes_by_address[address] = 0
                    changed_addresses.append(address)
                balance_changes_by_address[address] += out_amount_by_address[address]

        logger.info(f"Adding row(s)...", extra=logger_extra_data(add_rows=len(changed_addresses)))

        new_rows = [BalanceChange(address=address, d_balance=balance_changes_by_address[address], block=block_height,
                                  block_timestamp=block_timestamp) for address in changed_addresses]

        with self.Session() as session:
            try:
                # Add the new rows to the balance_changes table
                session.add_all(new_rows)

                # Add new row to blocks table
                session.add(Block(block_height=block_height, timestamp=block_timestamp))

                # Commit the session to save the changes to the database
                session.commit()

                return True

            except SQLAlchemyError as e:
                # Rollback the session in case of an error
                session.rollback()
                logger.error(f"An exception occurred", extra=logger_extra_data(
                    error={'exception_type': e.__class__.__name__, 'exception_message': str(e),
                           'exception_args': e.args}))

                return False
