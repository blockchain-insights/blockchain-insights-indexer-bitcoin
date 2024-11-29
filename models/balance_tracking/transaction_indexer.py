from datetime import datetime
from typing import Dict, Any

from loguru import logger
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker


class TransactionIndexer:
    def __init__(self, db_url: str):
        engine = create_engine(db_url)
        self.SessionFactory = sessionmaker(bind=engine)

    def index_transaction(self, tx: Dict[str, Any], timestamp: int, block_height: int) -> None:
        """Index a single transaction with its inputs and outputs."""

        with self.SessionFactory() as session:
            try:
                result = session.execute(
                    text("""
                        INSERT INTO transactions (
                            tx_id, tx_index, timestamp, block_height, is_coinbase,
                            in_total_amount, out_total_amount, fee_amount
                        ) VALUES (
                            :tx_id, :tx_index, :timestamp, :block_height, :is_coinbase,
                            :in_total_amount, :out_total_amount, :fee_amount
                        )
                        ON CONFLICT (tx_id, timestamp) DO NOTHING
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

                if result.rowcount == 0:
                    logger.warning(f"Transaction already indexed", tx_id=tx["tx_id"])
                    return

                # Insert inputs
                if tx["vins"]:
                    session.execute(
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
                    session.execute(
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

                session.commit()

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to index transaction {tx['tx_id']}", error=str(e))
                raise
