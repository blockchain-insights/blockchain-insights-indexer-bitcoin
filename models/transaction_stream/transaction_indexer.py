import json
from datetime import datetime
from typing import Dict, Any, List, Tuple

from loguru import logger
from clickhouse_driver import Client


class TransactionIndexer:
    def __init__(self, connection_params: Dict[str, Any]):
        self.client = Client(**connection_params)
        self.batch_size = 10000  # Configurable batch size for optimal performance

    def _prepare_transaction_data(self, transactions: List[Dict[str, Any]]) -> List[Tuple]:
        """Prepare transaction data for bulk insert"""
        return [
            (
                tx["tx_id"],
                tx["tx_index"],
                datetime.fromtimestamp(tx["timestamp"]),
                tx["block_height"],
                tx["is_coinbase"],
                tx["in_total_amount"],
                tx["out_total_amount"],
                0 if tx["is_coinbase"] else tx["in_total_amount"] - tx["out_total_amount"],
                tx["size"],
                tx["vsize"],
                tx["weight"]
            )
            for tx in transactions
        ]

    def _prepare_input_data(self, transactions: List[Dict[str, Any]]) -> List[Tuple]:
        """Prepare inputs data for bulk insert"""
        inputs_data = []
        for tx in transactions:
            for vin in tx["vins"]:
                inputs_data.append((
                    len(inputs_data),  # Auto-incrementing id
                    vin["tx_id"],
                    vin["address"],
                    vin["amount"],
                    tx["block_height"]
                ))
        return inputs_data

    def _prepare_output_data(self, transactions: List[Dict[str, Any]]) -> List[Tuple]:
        """Prepare outputs data for bulk insert"""
        outputs_data = []
        for tx in transactions:
            for vout in tx["vouts"]:
                outputs_data.append((
                    len(outputs_data),  # Auto-incrementing id
                    vout["tx_id"],
                    vout["address"],
                    vout["amount"],
                    tx["block_height"]
                ))
        return outputs_data

    def index_transactions(self, transactions: List[Dict[str, Any]]):
        try:
            if not transactions:
                return

            # Prepare all data
            tx_data = self._prepare_transaction_data(transactions)
            input_data = self._prepare_input_data(transactions)
            output_data = self._prepare_output_data(transactions)

            # Bulk insert transactions
            self.client.execute(
                """
                INSERT INTO transactions (
                    tx_id, tx_index, block_timestamp, block_height, is_coinbase,
                    in_total_amount, out_total_amount, fee_amount, 
                    size, vsize, weight
                ) VALUES
                """,
                tx_data
            )
            logger.info(f"Inserted {len(tx_data)} transactions")

            # Bulk insert inputs if any
            if input_data:
                self.client.execute(
                    """
                    INSERT INTO transaction_inputs (
                        id, tx_id, address, amount, block_height
                    ) VALUES
                    """,
                    input_data
                )
                logger.info(f"Inserted {len(input_data)} transaction inputs")

            # Bulk insert outputs if any
            if output_data:
                self.client.execute(
                    """
                    INSERT INTO transaction_outputs (
                        id, tx_id, address, amount, block_height
                    ) VALUES
                    """,
                    output_data
                )
                logger.info(f"Inserted {len(output_data)} transaction outputs")

        except Exception as e:
            logger.error("Failed to index transactions batch", error=str(e))
            raise e

    def index_transactions_in_batches(self, transactions: List[Dict[str, Any]]):
        """Process transactions in batches for memory efficiency"""
        for i in range(0, len(transactions), self.batch_size):
            batch = transactions[i:i + self.batch_size]
            self.index_transactions(batch)
            logger.info(f"Processed batch {i // self.batch_size + 1}")
