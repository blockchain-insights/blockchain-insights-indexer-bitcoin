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

    def setup_db(self):
        """Initialize database if not using init.sql. This is a fallback initialization method."""
        with self.engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            try:
                # Create TimescaleDB extension
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                logger.info("TimescaleDB extension check completed")

                # Create block_height_now function
                conn.execute(text("""
                    CREATE OR REPLACE FUNCTION block_height_now()
                    RETURNS INTEGER LANGUAGE SQL STABLE AS
                    $$
                        SELECT 2147483647;  -- Maximum 32-bit integer
                    $$;
                """))
                logger.info("Created block_height_now function")

                # Create tables if they don't exist
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS balance_changes (
                        address TEXT NOT NULL,
                        block_height INTEGER NOT NULL,
                        event TEXT,
                        balance_delta BIGINT NOT NULL,
                        block_timestamp TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY(address, block_height)
                    );
                """))

                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS balance_address_blocks (
                        address TEXT NOT NULL,
                        block_height INTEGER NOT NULL,
                        balance BIGINT NOT NULL,
                        block_timestamp TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY(address, block_height)
                    );
                """))

                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS blocks (
                        block_height INTEGER NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY(block_height)
                    );
                """))
                logger.info("Created tables")

                # Convert to hypertables
                for table in ['balance_changes', 'balance_address_blocks']:
                    result = conn.execute(text(
                        f"SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = '{table}';"
                    )).fetchone()

                    if not result:
                        conn.execute(text(f"""
                            SELECT create_hypertable('{table}', 'block_height',
                                chunk_time_interval => 12960,
                                if_not_exists => TRUE,
                                migrate_data => TRUE,
                                create_default_indexes => FALSE
                            );
                        """))
                        conn.execute(text(f"SELECT set_integer_now_func('{table}', 'block_height_now');"))
                        logger.info(f"Created hypertable for {table}")

                # Create trigger function
                conn.execute(text("""
                    CREATE OR REPLACE FUNCTION update_balance_address_block()
                        RETURNS TRIGGER AS $func$
                        DECLARE
                            _last_balance BIGINT;
                            _last_block_height INTEGER;
                        BEGIN
                            SELECT balance, block_height
                            INTO _last_balance, _last_block_height
                            FROM balance_address_blocks
                            WHERE address = NEW.address
                            AND block_height < NEW.block_height
                            ORDER BY block_height DESC
                            LIMIT 1;

                            INSERT INTO balance_address_blocks (
                                address,
                                block_height,
                                block_timestamp,
                                balance
                            )
                            VALUES (
                                NEW.address,
                                NEW.block_height,
                                NEW.block_timestamp,
                                COALESCE(_last_balance, 0) + NEW.balance_delta
                            );

                            RETURN NEW;
                        END;
                        $func$ LANGUAGE plpgsql;
                """))

                # Create trigger
                conn.execute(text("""
                    DROP TRIGGER IF EXISTS balance_changes_after_insert ON balance_changes;
                    CREATE TRIGGER balance_changes_after_insert
                        AFTER INSERT ON balance_changes
                        FOR EACH ROW
                        EXECUTE FUNCTION update_balance_address_block();
                """))
                logger.info("Created trigger")

                # Create indexes if they don't exist
                indexes = [
                    ("idx_balance_changes_block_height", "balance_changes", "(block_height)"),
                    ("idx_balance_changes_addr_time", "balance_changes", "(address, block_timestamp DESC)"),
                    ("idx_bab_addr_height_lookup", "balance_address_blocks", "(address, block_height DESC)"),
                    ("idx_bab_height_balance_top", "balance_address_blocks",
                     "(block_height DESC, balance DESC) INCLUDE (address)"),
                    ("idx_bab_height_balance_range", "balance_address_blocks",
                     "(block_height DESC, balance) INCLUDE (address)"),
                    ("idx_bab_timestamp_balance", "balance_address_blocks",
                     "(block_timestamp DESC, balance) INCLUDE (address, block_height)"),
                    ("idx_bab_addr_time", "balance_address_blocks",
                     "(address, block_timestamp DESC) INCLUDE (balance, block_height)"),
                    ("idx_blocks_timestamp", "blocks", "(timestamp)")
                ]

                for idx_name, table, definition in indexes:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} {definition};"))
                logger.info("Created indexes")

                # Enable compression
                for table in ['balance_changes', 'balance_address_blocks']:
                    conn.execute(text(f"""
                        ALTER TABLE {table} SET (
                            timescaledb.compress,
                            timescaledb.compress_segmentby = 'address',
                            timescaledb.compress_orderby = 'block_height DESC'
                        );
                    """))
                logger.info("Enabled compression")

                # Create monitoring view and function
                conn.execute(text("""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS balance_tables_stats AS
                    WITH stats AS (
                        SELECT
                            date_trunc('hour', block_timestamp) as time_bucket,
                            COUNT(*) as total_records,
                            COUNT(DISTINCT address) as unique_addresses,
                            MIN(balance) as min_balance,
                            MAX(balance) as max_balance,
                            AVG(balance)::BIGINT as avg_balance,
                            percentile_cont(0.5) WITHIN GROUP (ORDER BY balance) as median_balance
                        FROM balance_address_blocks
                        GROUP BY date_trunc('hour', block_timestamp)
                    )
                    SELECT * FROM stats;

                    CREATE INDEX IF NOT EXISTS idx_balance_stats_time
                        ON balance_tables_stats (time_bucket DESC);
                """))

                # Create utility functions
                conn.execute(text("""
                    CREATE OR REPLACE FUNCTION refresh_balance_stats()
                        RETURNS void AS $$
                    BEGIN
                        REFRESH MATERIALIZED VIEW CONCURRENTLY balance_tables_stats;
                    END;
                    $$ LANGUAGE plpgsql;

                    CREATE OR REPLACE FUNCTION compress_old_chunks()
                    RETURNS void AS $$
                    BEGIN
                        PERFORM compress_chunk(chunk)
                        FROM show_chunks('balance_changes') AS chunk
                        WHERE chunk_relation_size(chunk) > 0;

                        PERFORM compress_chunk(chunk)
                        FROM show_chunks('balance_address_blocks') AS chunk
                        WHERE chunk_relation_size(chunk) > 0;
                    END;
                    $$ LANGUAGE plpgsql;
                """))
                logger.info("Created utility functions")

                logger.info("Database setup completed successfully")

            except SQLAlchemyError as e:
                logger.error(f"Database setup failed: {str(e)}")
                raise

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