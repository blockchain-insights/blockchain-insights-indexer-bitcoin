import os
import time
from decimal import Decimal
from pathlib import Path
from typing import Tuple, Optional, List
import duckdb
from loguru import logger
from models.block_stream_state import BlockStreamStateManager


class TransactionOutputCache:
    def __init__(
            self,
            data_dir: str = 'tx_cache_parquet',
            checkpoint_interval: int = 1000,
            parquet_filename: str = 'outputs.parquet',
            db_url: Optional[str] = None,
            bitcoin_node=None
    ):
        self.data_dir = Path(os.getenv('BLOCK_STREAM_TRANSACTION_CACHE', data_dir))
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.parquet_path = str(self.data_dir / parquet_filename)
        self.bitcoin_node = bitcoin_node
        logger.info("Initializing in-memory transaction cache")

        self.state_manager = BlockStreamStateManager(db_url) if db_url else None
        self.conn = duckdb.connect(":memory:")

        self.pending_outputs = []
        self.checkpoint_interval = checkpoint_interval
        self.block_count = 0
        self.current_block_height = None
        self.highest_cached_block = None

        self._initialize_tables()
        self._restore_from_parquet()
        if self.state_manager and self.bitcoin_node:
            self._validate_and_resync_cache()

    def _get_missing_block_ranges(self, last_indexed_block: int) -> List[tuple[int, int]]:
        """
        Get ranges of blocks that need to be resynced
        Returns list of tuples (start_block, end_block)
        """
        if self.highest_cached_block is None:
            return [(0, last_indexed_block)]

        # Get all block heights we have in cache
        cached_blocks = self.conn.execute("""
            SELECT DISTINCT block_height 
            FROM outputs 
            WHERE block_height <= ? 
            ORDER BY block_height
        """, [last_indexed_block]).fetchall()

        cached_heights = set(row[0] for row in cached_blocks)

        # Find missing ranges
        missing_ranges = []
        start = None

        for height in range(0, last_indexed_block + 1):
            if height not in cached_heights:
                if start is None:
                    start = height
            elif start is not None:
                missing_ranges.append((start, height - 1))
                start = None

        if start is not None:
            missing_ranges.append((start, last_indexed_block))

        return missing_ranges

    def _resync_missing_transactions(self, missing_ranges: List[tuple[int, int]]):
        """
        Resync missing transactions for given block ranges
        """
        total_blocks = sum(end - start + 1 for start, end in missing_ranges)
        logger.info(f"Starting resync of {total_blocks} missing blocks across {len(missing_ranges)} ranges")

        for start_height, end_height in missing_ranges:
            logger.info(f"Resyncing blocks {start_height} to {end_height}")

            try:
                # Process blocks in smaller chunks to manage memory
                CHUNK_SIZE = 100
                for chunk_start in range(start_height, end_height + 1, CHUNK_SIZE):
                    chunk_end = min(chunk_start + CHUNK_SIZE - 1, end_height)

                    blocks = self.bitcoin_node.get_blocks_by_height_range(chunk_start, chunk_end)
                    if not blocks:
                        logger.warning(f"No blocks returned for range {chunk_start}-{chunk_end}")
                        continue

                    for block in blocks:
                        for tx in block["tx"]:
                            self.cache_transaction(block["height"], tx)

                        if block["height"] % 100 == 0:
                            logger.info(f"Resynced block {block['height']}")

                    # Force write after each chunk
                    self._write_to_parquet()
                    self.block_count = 0  # Reset block count after forced write

            except Exception as e:
                logger.error(f"Error resyncing blocks {start_height}-{end_height}: {str(e)}")
                raise

        logger.info("Completed resyncing missing transactions")

    def _validate_and_resync_cache(self):
        """
        Validate cache consistency against block_stream_state and resync missing transactions
        """
        if not self.state_manager or not self.bitcoin_node:
            logger.warning("Missing state manager or bitcoin node, skipping cache validation")
            return

        try:
            last_indexed_block = self.state_manager.get_last_block_height()
            self.highest_cached_block = self._get_highest_cached_block()

            logger.info(
                "Validating cache consistency",
                last_indexed_block=last_indexed_block,
                highest_cached_block=self.highest_cached_block
            )

            # Get missing block ranges
            missing_ranges = self._get_missing_block_ranges(last_indexed_block)

            if missing_ranges:
                total_missing = sum(end - start + 1 for start, end in missing_ranges)
                logger.warning(
                    f"Found {total_missing} missing blocks across {len(missing_ranges)} ranges",
                    ranges=missing_ranges
                )

                # Resync missing transactions
                self._resync_missing_transactions(missing_ranges)

                # Update highest cached block
                self.highest_cached_block = self._get_highest_cached_block()
                logger.info("Cache resync completed", highest_cached_block=self.highest_cached_block)
            else:
                logger.info("Cache is consistent with indexed state")

        except Exception as e:
            logger.error(f"Error validating and resyncing cache: {e}")
            raise

    def _initialize_tables(self):
        # Main outputs table for querying
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS outputs (
                txid VARCHAR,
                vout_index INTEGER,
                address VARCHAR,
                amount BIGINT,
                block_height INTEGER  -- Added block height for tracking
            )
        """)

        # Temporary table for staging new outputs
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_outputs (
                txid VARCHAR,
                vout_index INTEGER,
                address VARCHAR,
                amount BIGINT,
                block_height INTEGER
            )
        """)

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_outputs_txid_vout ON outputs(txid, vout_index)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_outputs_block_height ON outputs(block_height)")
        logger.info("Initialized in-memory transaction outputs tables and indexes")

    def _get_highest_cached_block(self) -> Optional[int]:
        """Get the highest block height from the cache"""
        result = self.conn.execute("""
            SELECT MAX(block_height) FROM outputs
        """).fetchone()
        return result[0] if result and result[0] is not None else None

    def _validate_cache_consistency(self):
        """
        Validate cache consistency against block_stream_state and handle any gaps
        """
        if not self.state_manager:
            logger.warning("No state manager provided, skipping cache validation")
            return

        try:
            last_indexed_block = self.state_manager.get_last_block_height()
            self.highest_cached_block = self._get_highest_cached_block()

            if self.highest_cached_block is None:
                logger.info("Empty transaction cache, no validation needed")
                return

            logger.info(
                "Validating cache consistency",
                last_indexed_block=last_indexed_block,
                highest_cached_block=self.highest_cached_block
            )

            if last_indexed_block > self.highest_cached_block:
                logger.warning(
                    "Cache is behind indexed state. Some transactions may need to be re-cached",
                    cache_gap=(last_indexed_block - self.highest_cached_block)
                )
                # Could implement re-caching logic here if needed

            elif last_indexed_block < self.highest_cached_block:
                logger.warning(
                    "Cache is ahead of indexed state. Trimming excess cached data",
                    excess_blocks=(self.highest_cached_block - last_indexed_block)
                )
                # Remove data for blocks that haven't been fully indexed
                self.conn.execute("""
                    DELETE FROM outputs 
                    WHERE block_height > ?
                """, [last_indexed_block])
                self.highest_cached_block = last_indexed_block

        except Exception as e:
            logger.error(f"Error validating cache consistency: {e}")
            raise

    def _restore_from_parquet(self):
        """
        Restore data from parquet file into the in-memory table
        """
        try:
            if os.path.exists(self.parquet_path):
                logger.info(f"Restoring data from {self.parquet_path}")

                file_size = os.path.getsize(self.parquet_path) / (1024 * 1024)
                logger.info(f"Parquet file size: {file_size:.2f} MB")

                before_count = self.conn.execute("SELECT COUNT(*) FROM outputs").fetchone()[0]

                self.conn.execute(f"""
                    INSERT INTO outputs 
                    SELECT * FROM parquet_scan('{self.parquet_path}')
                """)

                after_count = self.conn.execute("SELECT COUNT(*) FROM outputs").fetchone()[0]
                rows_added = after_count - before_count

                self.highest_cached_block = self._get_highest_cached_block()
                logger.info(
                    f"Restored {rows_added} records into in-memory table",
                    highest_block=self.highest_cached_block
                )
            else:
                logger.info("No existing parquet file found, starting with empty cache")

        except Exception as e:
            logger.error(f"Error restoring from parquet: {e}")
            raise

    def cache_transaction(self, block_height: int, tx: dict):
        if self.current_block_height is None:
            self.current_block_height = block_height
        elif block_height != self.current_block_height:
            self._finish_block()
            self.current_block_height = block_height

        outputs = [
            (
                tx["txid"],
                vout["n"],
                self._derive_address(vout["scriptPubKey"]),
                int(Decimal(vout["value"]) * 100000000),
                block_height  # Added block_height to stored data
            )
            for vout in tx["vout"]
            if vout["scriptPubKey"].get("type") not in ["nonstandard", "nulldata"]
        ]

        if outputs:
            self.conn.executemany("""
                INSERT INTO outputs (txid, vout_index, address, amount, block_height)
                VALUES (?, ?, ?, ?, ?)
            """, outputs)

            self.conn.executemany("""
                INSERT INTO pending_outputs (txid, vout_index, address, amount, block_height)
                VALUES (?, ?, ?, ?, ?)
            """, outputs)

    def _write_to_parquet(self):
        """
        Append new changes to the parquet file instead of rewriting everything
        """
        logger.info(f"Appending new changes to parquet: {self.parquet_path}")

        try:
            pending_count = self.conn.execute("SELECT COUNT(*) FROM pending_outputs").fetchone()[0]

            if pending_count == 0:
                logger.info("No new changes to write")
                return

            if not os.path.exists(self.parquet_path):
                self.conn.execute(f"""
                    COPY (SELECT * FROM pending_outputs) 
                    TO '{self.parquet_path}' (FORMAT 'parquet')
                """)
            else:
                temp_parquet = str(self.data_dir / f'temp_{int(time.time())}.parquet')

                self.conn.execute(f"""
                    COPY (SELECT * FROM pending_outputs) 
                    TO '{temp_parquet}' (FORMAT 'parquet')
                """)

                self.conn.execute(f"""
                    COPY (
                        SELECT * FROM parquet_scan('{self.parquet_path}')
                        UNION ALL
                        SELECT * FROM parquet_scan('{temp_parquet}')
                    ) TO '{self.parquet_path}' (FORMAT 'parquet')
                """)

                os.remove(temp_parquet)

            self.conn.execute("DELETE FROM pending_outputs")
            self.highest_cached_block = self._get_highest_cached_block()

            logger.info(
                f"Successfully appended {pending_count} new records to parquet file",
                highest_cached_block=self.highest_cached_block
            )

        except Exception as e:
            logger.error(f"Error writing to parquet: {e}")
            raise

    def _finish_block(self):
        """Handle block completion and checkpointing"""
        self.block_count += 1
        if self.block_count >= self.checkpoint_interval:
            self._write_to_parquet()
            self.block_count = 0

    def close(self):
        """Ensure all data is written before closing"""
        try:
            if self.current_block_height is not None:
                self._finish_block()

            # Force final write regardless of checkpoint interval
            self._write_to_parquet()

            # Close connections
            if self.state_manager:
                self.state_manager.close()
            self.conn.close()

            logger.info(
                "Closed transaction cache and wrote final snapshot",
                highest_cached_block=self.highest_cached_block
            )
        except Exception as e:
            logger.error(f"Error closing cache: {e}")
            raise

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def get_output(self, txid: str, vout_index: int) -> Tuple[str, int]:
        """Get transaction output data by txid and output index"""
        result = self.conn.execute("""
            SELECT address, amount 
            FROM outputs
            WHERE txid = ? AND vout_index = ?
            LIMIT 1
        """, [txid, vout_index]).fetchone()

        if result:
            return result[0], result[1]
        return f"unknown-{txid}", 0

    def _derive_address(self, script_pubkey: dict) -> str:
        """Extract address from scriptPubKey"""
        if "address" in script_pubkey:
            return script_pubkey["address"]
        elif "addresses" in script_pubkey and script_pubkey["addresses"]:
            return script_pubkey["addresses"][0]
        return "unknown"