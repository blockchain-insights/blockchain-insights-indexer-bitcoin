import os
from datetime import datetime
from typing import Optional

from setup_logger import setup_logger, logger_extra_data
import sqlalchemy as sa
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from .balance_model import BalanceChange, Block, BalanceAddressBlock

logger = setup_logger("BalanceIndexer")


class BalanceIndexer:
    def __init__(self, db_url: Optional[str] = None):
        if db_url is None:
            self.db_url = os.environ.get(
                "DB_CONNECTION_STRING",
                "postgresql://postgres:changeit456$@localhost:5432/miner"
            )
        else:
            self.db_url = db_url

        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

        # Verify database setup
        self._verify_database_setup()

    def _verify_database_setup(self):
        """Verify that the database is properly initialized."""
        with self.engine.connect() as conn:
            try:
                # Check TimescaleDB extension
                result = conn.execute(text(
                    "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';"
                )).fetchone()
                if not result:
                    logger.error("TimescaleDB extension not found. Please run init.sql first.")
                    raise RuntimeError("TimescaleDB extension not found")

                # Check hypertables
                for table in ['balance_changes', 'balance_address_blocks']:
                    result = conn.execute(text(
                        f"SELECT 1 FROM timescaledb_information.hypertables "
                        f"WHERE hypertable_name = '{table}';"
                    )).fetchone()
                    if not result:
                        logger.error(f"Hypertable {table} not found. Please run init.sql first.")
                        raise RuntimeError(f"Hypertable {table} not found")

                # Verify required indexes exist
                inspector = inspect(self.engine)

                # Check balance_changes indexes
                bc_indexes = {idx['name'] for idx in inspector.get_indexes('balance_changes')}
                required_bc_indexes = {
                    'idx_balance_changes_block_height',
                    'idx_balance_changes_addr_time'
                }

                # Check balance_address_blocks indexes
                bab_indexes = {idx['name'] for idx in inspector.get_indexes('balance_address_blocks')}
                required_bab_indexes = {
                    'idx_bab_addr_height_lookup',
                    'idx_bab_height_balance_top',
                    'idx_bab_height_balance_range',
                    'idx_bab_timestamp_balance',
                    'idx_bab_addr_time'
                }

                missing_indexes = (required_bc_indexes - bc_indexes) | (required_bab_indexes - bab_indexes)
                if missing_indexes:
                    logger.warning(f"Missing indexes: {missing_indexes}. Please run init.sql.")

                # Verify block_height_now function exists
                result = conn.execute(text(
                    "SELECT 1 FROM pg_proc WHERE proname = 'block_height_now';"
                )).fetchone()
                if not result:
                    logger.error("block_height_now function not found. Please run init.sql first.")
                    raise RuntimeError("block_height_now function not found")

            except SQLAlchemyError as e:
                logger.error(f"Database verification failed: {str(e)}")
                raise

    def close(self):
        self.engine.dispose()

    def get_latest_block_number(self):
        with self.Session() as session:
            try:
                latest_block = session.query(sa.func.max(BalanceChange.block_height)).scalar()
                return latest_block or 0
            except SQLAlchemyError as e:
                logger.error(f"Error getting latest block: {e}")
                return 0

    def create_rows_focused_on_balance_changes(self, block_data, _bitcoin_node):
        block_height = block_data.block_height
        block_timestamp = datetime.utcfromtimestamp(block_data.timestamp)
        transactions = block_data.transactions

        balance_changes_by_address = {}
        changed_addresses = {}

        for tx in transactions:
            in_amount_by_address, out_amount_by_address, input_addresses, output_addresses, in_total_amount, _ = (
                _bitcoin_node.process_in_memory_txn_for_indexing(tx)
            )

            event_type = 'coinbase' if tx.is_coinbase else 'transfer'

            for address in input_addresses:
                if address not in balance_changes_by_address:
                    balance_changes_by_address[address] = 0
                    changed_addresses[address] = event_type
                balance_changes_by_address[address] -= in_amount_by_address[address]

            for address in output_addresses:
                if address not in balance_changes_by_address:
                    balance_changes_by_address[address] = 0
                    changed_addresses[address] = event_type
                balance_changes_by_address[address] += out_amount_by_address[address]

        logger.info(f"Adding row(s)...", extra=logger_extra_data(add_rows=len(changed_addresses)))

        with self.Session() as session:
            try:
                # Create balance changes rows
                balance_changes = [
                    BalanceChange(
                        address=address,
                        balance_delta=balance_changes_by_address[address],
                        block_height=block_height,
                        block_timestamp=block_timestamp,
                        event=changed_addresses[address]
                    )
                    for address in changed_addresses
                ]

                # Add block record
                block = Block(
                    block_height=block_height,
                    timestamp=block_timestamp
                )

                session.add_all(balance_changes)
                session.add(block)
                session.commit()
                return True

            except SQLAlchemyError as e:
                session.rollback()
                logger.error(
                    "An exception occurred",
                    extra=logger_extra_data(
                        error={
                            'exception_type': e.__class__.__name__,
                            'exception_message': str(e),
                            'exception_args': e.args
                        }
                    )
                )
                return False

    def compress_chunks(self):
        """Trigger manual compression of old chunks."""
        with self.engine.connect() as conn:
            try:
                conn.execute(text("SELECT compress_old_chunks()"))
                logger.info("Successfully compressed old chunks")
            except SQLAlchemyError as e:
                logger.error(f"Error compressing chunks: {e}")