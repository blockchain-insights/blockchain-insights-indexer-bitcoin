import os
import sys
import signal
import threading
import time
import json
import traceback
from decimal import Decimal
from confluent_kafka.admin import AdminClient
from confluent_kafka import admin
from confluent_kafka import Producer
from loguru import logger
from typing import Dict, List, Tuple, Optional
from models.block_range_partitioner import BlockRangePartitioner
from models.block_stream_state import BlockStreamStateManager
from node.node import BitcoinNode


class TransactionCache:
    def __init__(self, cleanup_threshold: int = 1000):
        self.outputs: Dict[str, Dict[str, Dict[str, any]]] = {}
        self.tx_block_height: Dict[str, int] = {}  # Track block height for each transaction
        self.cleanup_threshold = cleanup_threshold
        self.lowest_block_in_window = None

    def add_transaction(self, tx: dict, block_height: int):
        """Add transaction outputs to cache"""
        self.outputs[tx["txid"]] = {
            str(vout["n"]): {
                "address": self._derive_address(vout["scriptPubKey"]),
                "amount": int(Decimal(vout["value"]) * 100000000)  # Convert to satoshi
            }
            for vout in tx["vout"]
            if vout["scriptPubKey"].get("type") not in ["nonstandard", "nulldata"]
        }
        self.tx_block_height[tx["txid"]] = block_height

        # Update lowest block height in window
        if self.lowest_block_in_window is None or block_height < self.lowest_block_in_window:
            self.lowest_block_in_window = block_height

    def get_output(self, txid: str, vout_id: str) -> Tuple[str, int]:
        """Get transaction output from cache"""
        tx_data = self.outputs.get(txid, {})
        vout_data = tx_data.get(str(vout_id), {})
        return (
            vout_data.get("address", f"unknown-{txid}"),
            vout_data.get("amount", 0)
        )

    def cleanup_old_transactions(self, current_window_start: int):
        """Remove transactions from blocks before the current window"""
        if len(self.outputs) > self.cleanup_threshold:
            txids_to_remove = [
                txid for txid, height in self.tx_block_height.items()
                if height < current_window_start
            ]

            for txid in txids_to_remove:
                del self.outputs[txid]
                del self.tx_block_height[txid]

            if txids_to_remove:
                logger.debug(
                    "Cleaned up old transactions from cache",
                    removed_count=len(txids_to_remove),
                    remaining_count=len(self.outputs),
                    current_window_start=current_window_start
                )

    def _derive_address(self, script_pubkey: dict) -> str:
        """Derive address from scriptPubKey"""
        if "address" in script_pubkey:
            return script_pubkey["address"]
        elif "addresses" in script_pubkey and script_pubkey["addresses"]:
            return script_pubkey["addresses"][0]
        return "unknown"


class BlockWindow:
    def __init__(self, size: int = 3, safety_margin: int = 6):
        self.size = size
        self.safety_margin = safety_margin
        self.cache = TransactionCache()
        self.blocks: List[dict] = []
        self.start_height: Optional[int] = None
        self.end_height: Optional[int] = None

    def update(self, blocks: List[dict]):
        """Update window with new blocks"""
        self.blocks = blocks

        if blocks:
            self.start_height = blocks[0]["height"]
            self.end_height = blocks[-1]["height"]

            # Cache all transaction outputs in window
            for block in blocks:
                block_height = block["height"]
                for tx in block["tx"]:
                    self.cache.add_transaction(tx, block_height)  # Fixed: Added block_height parameter

            # Cleanup old transactions, keeping safety margin
            safe_cleanup_height = self.start_height - self.safety_margin
            self.cache.cleanup_old_transactions(safe_cleanup_height)

    def contains_height(self, height: int) -> bool:
        """Check if height is within current window"""
        return (self.start_height is not None and
                self.end_height is not None and
                self.start_height <= height <= self.end_height)
