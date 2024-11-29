from datetime import datetime
from typing import Dict, Any

from loguru import logger
from sqlalchemy import text, create_engine


class TransactionIndexer:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self._engine = create_engine(db_url)

    def index_transaction(self, tx: Dict[str, Any], timestamp: int, block_height: int) -> None:
        try:
            with self._engine.begin() as conn:
                try:
                    # Insert main transaction
                    conn.execute(
                        text("""
                                INSERT INTO transactions (
                                    tx_id, tx_index, timestamp, block_height, is_coinbase,
                                    in_total_amount, out_total_amount, fee_amount
                                ) VALUES (
                                    :tx_id, :tx_index, :timestamp, :block_height, :is_coinbase,
                                    :in_total_amount, :out_total_amount, :fee_amount
                                )
                            """),
                        {
                            "tx_id": tx["tx_id"],
                            "tx_index": tx["tx_index"],
                            "timestamp": datetime.fromtimestamp(timestamp),
                            "block_height": block_height,
                            "is_coinbase": tx["is_coinbase"],
                            "in_total_amount": tx["in_total_amount"],
                            "out_total_amount": tx["out_total_amount"],
                            "fee_amount": tx["in_total_amount"] - tx["out_total_amount"]
                        }
                    )

                    # Insert inputs
                    if tx["vins"]:
                        conn.execute(
                            text("""
                                    INSERT INTO transaction_inputs (
                                        tx_id, timestamp, address, amount
                                    ) VALUES (
                                        :tx_id, :timestamp, :address, :amount
                                    )
                                """),
                            [{
                                "tx_id": tx["tx_id"],
                                "timestamp": datetime.fromtimestamp(timestamp),
                                "address": vin["address"],
                                "amount": vin["amount"]
                            } for vin in tx["vins"]]
                        )

                    # Insert outputs
                    if tx["vouts"]:
                        conn.execute(
                            text("""
                                    INSERT INTO transaction_outputs (
                                        tx_id, timestamp, address, amount
                                    ) VALUES (
                                        :tx_id, :timestamp, :address, :amount
                                    )
                                """),
                            [{
                                "tx_id": tx["tx_id"],
                                "timestamp": datetime.fromtimestamp(timestamp),
                                "address": vout["address"],
                                "amount": vout["amount"]
                            } for vout in tx["vouts"]]
                        )

                except Exception as e:
                    # The with block will automatically rollback on exception
                    raise  # Re-raise the exception to be caught by outer try block

        except Exception as e:
            logger.error(f"Failed to index transaction", error=e, tx_id=tx['tx_id'])
            raise
