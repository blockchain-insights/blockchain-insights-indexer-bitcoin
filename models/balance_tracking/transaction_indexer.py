from datetime import datetime
from typing import Dict, Any, List, Tuple
from loguru import logger
import clickhouse_connect


class TransactionIndexer:
    def __init__(self, connection_params: Dict[str, Any]):
        # Convert connection params for clickhouse_connect
        self.client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),  # port needs to be int
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database']
        )

    def _prepare_transaction_data(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare transaction data for bulk insert"""
        return [
            {
                'block_height': tx["block_height"],
                'block_timestamp': datetime.fromtimestamp(tx["timestamp"]),
                'tx_id': tx["tx_id"],
                'tx_index': tx["tx_index"],
                'is_coinbase': tx["is_coinbase"],
                'in_total_amount': tx["in_total_amount"],
                'out_total_amount': tx["out_total_amount"],
                'fee_amount': 0 if tx["is_coinbase"] else tx["in_total_amount"] - tx["out_total_amount"],
                'size': tx["size"],
                'vsize': tx["vsize"],
                'weight': tx["weight"]
            }
            for tx in transactions
        ]

    def _prepare_input_data(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare inputs data for bulk insert"""
        inputs_data = []
        id_counter = 0
        for tx in transactions:
            for vin in tx["vins"]:
                inputs_data.append({
                    'id': id_counter,
                    'tx_id': vin["tx_id"],
                    'address': vin["address"],
                    'amount': vin["amount"],
                    'block_height': tx["block_height"]
                })
                id_counter += 1
        return inputs_data

    def _prepare_output_data(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare outputs data for bulk insert"""
        outputs_data = []
        id_counter = 0
        for tx in transactions:
            for vout in tx["vouts"]:
                outputs_data.append({
                    'id': id_counter,
                    'tx_id': vout["tx_id"],
                    'address': vout["address"],
                    'amount': vout["amount"],
                    'block_height': tx["block_height"]
                })
                id_counter += 1
        return outputs_data

    def index_transactions(self, transactions: List[Dict[str, Any]]):
        try:
            if not transactions:
                return

            # Prepare all data
            tx_data = self._prepare_transaction_data(transactions)
            input_data = self._prepare_input_data(transactions)
            output_data = self._prepare_output_data(transactions)

            # Extract columns and data for transactions
            columns = ['block_height', 'block_timestamp', 'tx_id', 'tx_index',
                       'is_coinbase', 'in_total_amount', 'out_total_amount',
                       'fee_amount', 'size', 'vsize', 'weight']

            data = [
                [row[col] for col in columns]
                for row in tx_data
            ]

            # Insert transactions
            self.client.insert('transactions', data, column_names=columns)
            logger.info(f"Inserted {len(tx_data)} transactions")

            # Bulk insert inputs if any
            if input_data:
                input_columns = ['id', 'tx_id', 'address', 'amount', 'block_height']
                input_rows = [
                    [row[col] for col in input_columns]
                    for row in input_data
                ]
                self.client.insert('transaction_inputs', input_rows, column_names=input_columns)
                logger.info(f"Inserted {len(input_data)} transaction inputs")

            # Bulk insert outputs if any
            if output_data:
                output_columns = ['id', 'tx_id', 'address', 'amount', 'block_height']
                output_rows = [
                    [row[col] for col in output_columns]
                    for row in output_data
                ]
                self.client.insert('transaction_outputs', output_rows, column_names=output_columns)
                logger.info(f"Inserted {len(output_data)} transaction outputs")

        except Exception as e:
            logger.error(f"Failed to index transactions batch: {str(e)}")
            raise e

    def index_transactions_in_batches(self, transactions: List[Dict[str, Any]]):
        self.index_transactions(transactions)