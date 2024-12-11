import os
import time
from decimal import Decimal
from pathlib import Path
from typing import Tuple, Optional, List
import duckdb
from loguru import logger
from models.block_stream_state import BlockStreamStateManager
from node.node_utils import derive_address


class TransactionOutputCache:
    def __init__(
            self,
            data_dir: str = '../../block_stream_tx_cache',
            checkpoint_interval: int = 1000,
            blocks_per_file: int = 10000,
            db_url: Optional[str] = None,
            bitcoin_node=None,
            terminate_event=None
    ):
        self.data_dir = Path(os.getenv('BLOCK_STREAM_TRANSACTION_CACHE', data_dir))
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.blocks_per_file = blocks_per_file
        self.checkpoint_interval = checkpoint_interval
        self.bitcoin_node = bitcoin_node

        self.pending_block_heights = set()
        self.last_checkpoint_time = time.time()

        self.conn = duckdb.connect(":memory:")
        self.highest_cached_block = None
        self.state_manager = BlockStreamStateManager(db_url) if db_url else None
        self.terminate_event = terminate_event

        self._initialize_tables()
        self._restore_from_parquet_files()
        if self.state_manager and self.bitcoin_node:
            self._validate_and_resync_cache()

    def _initialize_tables(self):
        """Initialize the in-memory tables with optimized schema"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS outputs (
                tx_key VARCHAR,        -- Combined txid-vout, e.g. {txid}-{vout}
                address VARCHAR,
                amount BIGINT,
                block_height INTEGER
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_outputs (
                tx_key VARCHAR,
                address VARCHAR,
                amount BIGINT,
                block_height INTEGER
            )
        """)

        # Indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_outputs_tx_key ON outputs(tx_key)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_outputs_block_height ON outputs(block_height)")

        logger.info("Initialized in-memory transaction outputs tables with optimized schema")

    def _get_parquet_file_name(self, block_height: int) -> str:
        file_block = (block_height // self.blocks_per_file) * self.blocks_per_file
        return f"blocks_{file_block:07d}.parquet"

    def _get_parquet_path(self, block_height: int) -> str:
        return str(self.data_dir / self._get_parquet_file_name(block_height))

    def _get_highest_cached_block(self) -> Optional[int]:
        result = self.conn.execute("SELECT MAX(block_height) FROM outputs").fetchone()
        return result[0] if result and result[0] is not None else None

    def _restore_from_parquet_files(self):
        """Restore data from all parquet files in directory"""
        try:
            parquet_files = sorted(self.data_dir.glob("blocks_*.parquet"))
            if not parquet_files:
                logger.info("No existing parquet files found, starting with empty cache")
                return

            logger.info(f"Found {len(parquet_files)} parquet files to restore")

            for parquet_file in parquet_files:
                if self.terminate_event and self.terminate_event.is_set():
                    logger.info("Termination requested, stopping resync process")
                    return

                if os.path.exists(parquet_file):
                    file_size = os.path.getsize(parquet_file) / (1024 * 1024)
                    logger.info(f"Restoring data from {parquet_file} ({file_size:.2f} MB)")

                    self.conn.execute(f"""
                        INSERT INTO outputs 
                        SELECT * FROM parquet_scan('{parquet_file}')
                    """)

            total_records = self.conn.execute("SELECT COUNT(*) FROM outputs").fetchone()[0]
            self.highest_cached_block = self._get_highest_cached_block()

            logger.info(
                f"Restored {total_records} total records from {len(parquet_files)} files",
                highest_block=self.highest_cached_block
            )

        except Exception as e:
            logger.error(f"Error restoring from parquet files: {e}")
            raise

    def _get_missing_block_ranges(self, last_indexed_block: int) -> List[tuple[int, int]]:
        if self.highest_cached_block is None:
            return [(0, last_indexed_block)]

        cached_blocks = self.conn.execute("""
            SELECT DISTINCT block_height 
            FROM outputs 
            WHERE block_height <= ? 
            ORDER BY block_height
        """, [last_indexed_block]).fetchall()

        cached_heights = set(row[0] for row in cached_blocks)
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

    def _validate_and_resync_cache(self):
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

            missing_ranges = self._get_missing_block_ranges(last_indexed_block)

            if missing_ranges:
                total_missing = sum(end - start + 1 for start, end in missing_ranges)
                logger.warning(
                    f"Found {total_missing} missing blocks across {len(missing_ranges)} ranges",
                    ranges=missing_ranges
                )
                self._resync_missing_transactions(missing_ranges)
                self.highest_cached_block = self._get_highest_cached_block()
                logger.info("Cache resync completed", highest_cached_block=self.highest_cached_block)
            else:
                logger.info("Cache is consistent with indexed state")

        except Exception as e:
            logger.error(f"Error validating and resyncing cache: {e}")
            raise

    def _get_batch_size_for_block(self, current_height: int) -> int:
        max_height = 850000
        max_batch = 8
        min_batch = 1

        if current_height >= max_height:
            return min_batch

        fraction = current_height / max_height
        batch_size = max(min_batch, round(max_batch - (max_batch - min_batch) * fraction))
        return batch_size

    def _resync_missing_transactions(self, missing_ranges: List[tuple[int, int]]):
        """Resync missing transactions for given block ranges"""
        total_blocks = sum(end - start + 1 for start, end in missing_ranges)
        logger.info(f"Starting resync of {total_blocks} missing blocks across {len(missing_ranges)} ranges")

        for start_height, end_height in missing_ranges:
            logger.info(f"Resyncing blocks {start_height} to {end_height}")

            try:
                current_height = start_height
                while current_height <= end_height:
                    if self.terminate_event and self.terminate_event.is_set():
                        logger.info("Termination requested, stopping resync process")
                        return

                    batch_size = self._get_batch_size_for_block(current_height)
                    upper_bound = min(current_height + batch_size - 1, end_height)

                    blocks = self.bitcoin_node.get_blocks_by_height_range(current_height, upper_bound)
                    if not blocks:
                        logger.warning(f"No blocks returned for range {current_height}-{upper_bound}")
                        current_height = upper_bound + 1
                        continue

                    bulk_outputs = []
                    block_heights = set()
                    for block in blocks:
                        if self.terminate_event and self.terminate_event.is_set():
                            logger.info("Termination requested, stopping resync process")
                            return
                        block_height = block["height"]
                        for tx in block["tx"]:
                            bulk_outputs.extend(self._transform_tx_outputs(block_height, tx))
                        block_heights.add(block_height)

                    if bulk_outputs:
                        start_time = time.time()
                        self._bulk_insert_outputs(bulk_outputs)
                        end_time = time.time()

                        inserted_count = len(bulk_outputs)
                        elapsed = end_time - start_time
                        tps = inserted_count / elapsed if elapsed > 0 else inserted_count
                        logger.info(
                            f"Inserted {inserted_count} tx cache outputs in {elapsed:.4f}s (TPS: {tps:.2f}), "
                            f"batch_size={batch_size}, current_height={current_height}"
                        )

                        for bh in block_heights:
                            self.pending_block_heights.add(bh)
                        self._check_checkpoint_needed(max(block_heights))

                    current_height = upper_bound + 1

                self._write_pending_to_parquet()

            except Exception as e:
                logger.error(f"Error resyncing blocks {start_height}-{end_height}: {str(e)}")
                raise

        logger.info("Completed resyncing missing transactions")

    def _transform_tx_outputs(self, block_height: int, tx: dict) -> List[tuple]:
        """Transform a single transaction into a list of output records with composite key"""
        outputs = []
        for vout in tx["vout"]:
            if vout["scriptPubKey"].get("type") not in ["nonstandard", "nulldata"]:
                tx_key = f"{tx['txid']}-{vout['n']}"
                address = self._derive_address(vout["scriptPubKey"])
                amount = int(Decimal(vout["value"]) * 100000000)
                outputs.append((tx_key, address, amount, block_height))
        return outputs

    def _bulk_insert_outputs(self, outputs: List[tuple]):
        """Perform a bulk insert of outputs with composite key"""
        if not outputs:
            return

        self.conn.executemany("""
            INSERT INTO outputs (tx_key, address, amount, block_height)
            VALUES (?, ?, ?, ?)
        """, outputs)

        self.conn.executemany("""
            INSERT INTO pending_outputs (tx_key, address, amount, block_height)
            VALUES (?, ?, ?, ?)
        """, outputs)

    def _check_checkpoint_needed(self, current_block_height: int):
        """Determine if we need to checkpoint based on blocks or time"""
        should_checkpoint = False

        if len(self.pending_block_heights) >= self.checkpoint_interval:
            should_checkpoint = True
            logger.info(f"Checkpoint triggered by block count: {len(self.pending_block_heights)} blocks pending")

        time_since_last_checkpoint = time.time() - self.last_checkpoint_time
        if time_since_last_checkpoint > 300:  # 5 minutes
            should_checkpoint = True
            logger.info(f"Checkpoint triggered by time: {time_since_last_checkpoint:.1f} seconds since last checkpoint")

        if should_checkpoint:
            self._write_pending_to_parquet()
            self.pending_block_heights.clear()
            self.last_checkpoint_time = time.time()

    def _write_pending_to_parquet(self):
        """Write pending outputs to parquet files"""
        try:
            pending_count = self.conn.execute("SELECT COUNT(*) FROM pending_outputs").fetchone()[0]
            if pending_count == 0:
                return

            min_block = min(self.pending_block_heights)
            max_block = max(self.pending_block_heights)

            min_file_block = (min_block // self.blocks_per_file) * self.blocks_per_file
            max_file_block = (max_block // self.blocks_per_file) * self.blocks_per_file

            logger.info(
                "Writing checkpoint",
                pending_blocks=len(self.pending_block_heights),
                block_range=(min_block, max_block),
                file_blocks=range(min_file_block, max_file_block + self.blocks_per_file, self.blocks_per_file)
            )

            for file_block in range(min_file_block, max_file_block + self.blocks_per_file, self.blocks_per_file):
                parquet_path = self._get_parquet_path(file_block)
                block_range_start = file_block
                block_range_end = file_block + self.blocks_per_file - 1

                temp_path = str(self.data_dir / f'temp_{file_block}_{int(time.time())}.parquet')

                self.conn.execute(f"""
                    COPY (
                        SELECT tx_key, address, amount, block_height
                        FROM pending_outputs 
                        WHERE block_height >= {block_range_start} 
                        AND block_height <= {block_range_end}
                    ) TO '{temp_path}' (FORMAT 'parquet')
                """)

                if os.path.exists(parquet_path):
                    # Merge existing data with new pending data
                    self.conn.execute(f"""
                        COPY (
                            SELECT * FROM parquet_scan('{parquet_path}')
                            UNION ALL
                            SELECT * FROM parquet_scan('{temp_path}')
                        ) TO '{parquet_path}' (FORMAT 'parquet')
                    """)
                    os.remove(temp_path)
                else:
                    os.rename(temp_path, parquet_path)

            self.conn.execute("DELETE FROM pending_outputs")
            self.highest_cached_block = max_block

            logger.info(
                f"Checkpoint complete",
                records_written=pending_count,
                highest_block=self.highest_cached_block
            )

        except Exception as e:
            logger.error(f"Error writing checkpoint: {e}")
            raise

    def cache_block(self, blocks):
        all_outputs = []
        block_heights = set()
        for block in blocks:
            block_heights.add(block['height'])
            for tx in block["tx"]:
                outputs = self._transform_tx_outputs(block['height'], tx)
                if outputs:
                    all_outputs.extend(outputs)

        if all_outputs:
            self._bulk_insert_outputs(all_outputs)
            for block_height in block_heights:
                self.pending_block_heights.add(block_height)
                self._check_checkpoint_needed(block_height)

    def cache_transaction(self, block_height: int, tx: dict):
        """Cache a single transaction"""
        outputs = self._transform_tx_outputs(block_height, tx)
        if outputs:
            self._bulk_insert_outputs(outputs)
            self.pending_block_heights.add(block_height)
            self._check_checkpoint_needed(block_height)

    def get_output(self, txid: str, vout_index: int) -> Tuple[str, int]:
        """Lookup output using composite key"""
        tx_key = f"{txid}-{vout_index}"
        result = self.conn.execute("""
            SELECT address, amount 
            FROM outputs
            WHERE tx_key = ?
            LIMIT 1
        """, [tx_key]).fetchone()

        if result:
            return result[0], result[1]
        return f"unknown-{txid}", 0

    def _derive_address(self, script_pubkey: dict) -> str:
        script_type = script_pubkey.get("type", "unknown")
        if script_type == "nulldata":
            return f"nulldata-{script_type}"

        address = derive_address(script_pubkey, script_pubkey.get("asm", ""))
        if address.startswith("unknown-") or address.startswith("UNKNOWN_"):
            raise ValueError(f"Could not derive address for script type {script_type}: {script_pubkey}")
        return address
