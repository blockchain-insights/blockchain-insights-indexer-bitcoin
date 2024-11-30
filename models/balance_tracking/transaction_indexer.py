import json
from datetime import datetime
from typing import Dict, Any

from loguru import logger
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker


class TransactionIndexer:
    def __init__(self, db_url: str):
        engine = create_engine(db_url)
        self.SessionFactory = sessionmaker(bind=engine)

    def index_transaction(self, tx: Dict[str, Any]) -> None:
        with self.SessionFactory() as session:
            try:
                tx["timestamp"] = datetime.fromtimestamp(tx["timestamp"]).isoformat()

                result = session.execute(
                    text("""
                        WITH tx_insert AS (
                            INSERT INTO transactions (
                                tx_id, tx_index, timestamp, block_height, is_coinbase,
                                in_total_amount, out_total_amount, fee_amount
                            ) 
                            SELECT 
                                tx_id, tx_index, timestamp::timestamptz, block_height, is_coinbase,
                                in_total_amount, out_total_amount,
                                CASE WHEN is_coinbase THEN 0 
                                     ELSE in_total_amount - out_total_amount 
                                END as fee_amount
                            FROM json_to_record(:tx) AS d(
                                tx_id varchar, tx_index int, timestamp varchar, 
                                block_height int, is_coinbase boolean,
                                in_total_amount numeric, out_total_amount numeric
                            )
                            ON CONFLICT (tx_id, timestamp) DO NOTHING
                            RETURNING timestamp, tx_id
                        ),
                        input_insert AS (
                            INSERT INTO transaction_inputs (tx_id, timestamp, address, amount)
                            SELECT d.tx_id, t.timestamp, d.address, d.amount
                            FROM json_to_recordset(:tx_vins) AS d(tx_id varchar, address varchar, amount numeric)
                            CROSS JOIN tx_insert t
                            WHERE EXISTS (SELECT 1 FROM tx_insert)
                        ),
                        output_insert AS (
                            INSERT INTO transaction_outputs (tx_id, timestamp, address, amount)
                            SELECT d.tx_id, t.timestamp, d.address, d.amount
                            FROM json_to_recordset(:tx_vouts) AS d(tx_id varchar, address varchar, amount numeric)
                            CROSS JOIN tx_insert t
                            WHERE EXISTS (SELECT 1 FROM tx_insert)
                        )
                        SELECT EXISTS (SELECT 1 FROM tx_insert) as inserted
                    """),
                    {
                        "tx": json.dumps(tx),
                        "tx_vins": json.dumps(tx["vins"]),
                        "tx_vouts": json.dumps(tx["vouts"])
                    }
                )

                inserted = result.scalar()
                if not inserted:
                    logger.warning(f"Transaction already indexed", tx_id=tx["tx_id"])
                    return

                session.commit()

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to index transaction {tx['tx_id']}", error=str(e))
                raise