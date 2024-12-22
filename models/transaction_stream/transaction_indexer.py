from datetime import datetime
from typing import Dict, Any, List
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
        self.version = int(datetime.now().strftime('%Y%m%d%H%M%S'))

    def _prepare_blocks_data(self, transactions: List[Dict[str, Any]]) -> List[List[Any]]:
        """Extract unique blocks data from transactions"""
        # Use a set to track unique block heights we've seen
        seen_blocks = set()
        blocks_data = []

        for tx in transactions:
            block_height = tx["block_height"]
            # Only process each block once
            if block_height not in seen_blocks:
                blocks_data.append([
                    block_height,
                    datetime.fromtimestamp(tx["timestamp"]),
                    self.version
                ])
                seen_blocks.add(block_height)

        # Sort by block height for consistency
        return sorted(blocks_data, key=lambda x: x[0])

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
                'weight': tx["weight"],
                '_version': self.version
            }
            for tx in transactions
        ]

    def _prepare_input_data(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare inputs data for bulk insert"""
        inputs_data = []
        for tx in transactions:
            for vin_index, vin in enumerate(tx["vins"]):
                inputs_data.append({
                    'tx_id': vin["tx_id"],
                    'vin_index': vin_index,
                    'address': vin["address"],
                    'amount': vin["amount"],
                    'block_height': tx["block_height"],
                    '_version': self.version
                })
        return inputs_data

    def _prepare_output_data(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare outputs data for bulk insert"""
        outputs_data = []
        for tx in transactions:
            for vout_index, vout in enumerate(tx["vouts"]):
                outputs_data.append({
                    'tx_id': vout["tx_id"],
                    'vout_index': vout_index,
                    'address': vout["address"],
                    'amount': vout["amount"],
                    'block_height': tx["block_height"],
                    '_version': self.version
                })
        return outputs_data

    def index_transactions(self, transactions: List[Dict[str, Any]]):
        try:
            if not transactions:
                return

            block_data = self._prepare_blocks_data(transactions)
            tx_data = self._prepare_transaction_data(transactions)
            input_data = self._prepare_input_data(transactions)
            output_data = self._prepare_output_data(transactions)

            if block_data:
                block_columns = ['block_height', 'block_timestamp', '_version']
                self.client.insert('blocks', block_data, column_names=block_columns)
                logger.info(f"Inserted {len(block_data)} blocks")

            if tx_data:
                columns = ['block_height', 'block_timestamp', 'tx_id', 'tx_index',
                           'is_coinbase', 'in_total_amount', 'out_total_amount',
                           'fee_amount', 'size', 'vsize', 'weight', '_version']

                data = [
                    [row[col] for col in columns]
                    for row in tx_data
                ]

                self.client.insert('transactions', data, column_names=columns)
                logger.info(f"Inserted {len(tx_data)} transactions")

            if input_data:
                input_columns = ['tx_id', 'address', 'amount', 'block_height', '_version']
                input_rows = [
                    [row[col] for col in input_columns]
                    for row in input_data
                ]
                self.client.insert('transaction_inputs', input_rows, column_names=input_columns)
                logger.info(f"Inserted {len(input_data)} transaction inputs")

            if output_data:
                output_columns = ['tx_id', 'address', 'amount', 'block_height', '_version']
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
