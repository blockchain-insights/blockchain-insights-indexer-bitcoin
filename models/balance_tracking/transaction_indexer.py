from typing import Dict, Any, List, Tuple, Set
from loguru import logger
import clickhouse_connect
from decimal import Decimal
from datetime import datetime

class TransactionIndexer:
    def __init__(self, connection_params: Dict[str, Any]):
        self.client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),  # port needs to be int
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database']
        )

        self.version = int(datetime.now().strftime('%Y%m%d%H%M%S'))

    def _collect_addresses(self, transactions: List[Dict[str, Any]]) -> Set[str]:
        """Collect all unique addresses from transactions inputs and outputs"""
        addresses = set()
        for tx in transactions:
            if not tx["is_coinbase"]:
                for vin in tx["vins"]:
                    addresses.add(vin["address"])
            for vout in tx["vouts"]:
                addresses.add(vout["address"])
        return addresses

    def _get_latest_balances(self, addresses: Set[str]) -> Dict[str, Tuple[int, Decimal]]:
        """Get latest balance and block height for each address"""
        query = """
       SELECT address, block_height, balance
       FROM balance_address_blocks
       WHERE address IN %(addresses)s
       ORDER BY block_height DESC
       LIMIT 1 BY address
       """
        results = self.client.query(query, parameters={
            'addresses': list(addresses)
        })

        balances = {}
        for row in results.named_results():
            address = row['address']
            balances[address] = (row['block_height'], Decimal(str(row['balance'])))
        return balances

    def _calculate_changes_and_balances(self, transactions: List[Dict[str, Any]],
                                        current_balances: Dict[str, Tuple[int, Decimal]]) -> Tuple[
        List[List[Any]], List[List[Any]]]:
        """Calculate balance changes and new balances"""
        changes = []
        new_balances = {}  # (address, block_height) -> balance

        sorted_txs = sorted(transactions, key=lambda x: x["block_height"])

        for tx in sorted_txs:
            block_height = tx["block_height"]

            # Process inputs (spending)
            if not tx["is_coinbase"]:
                for vin in tx["vins"]:
                    address = vin["address"]
                    amount = Decimal(str(vin["amount"]))

                    # Record the spend
                    changes.append([
                        address,
                        block_height,
                        tx["tx_id"],
                        tx["tx_index"],
                        'transfer',
                        -amount,
                        self.version
                    ])

                    # Update balance
                    last_balance = current_balances.get(address, (0, Decimal('0')))[1]
                    if (address, block_height) in new_balances:
                        last_balance = new_balances[(address, block_height)]
                    new_balances[(address, block_height)] = last_balance - amount

            # Process outputs (receiving)
            for vout in tx["vouts"]:
                address = vout["address"]
                amount = Decimal(str(vout["amount"]))

                # Record the receive
                changes.append([
                    address,
                    block_height,
                    tx["tx_id"],
                    tx["tx_index"],
                    'coinbase' if tx["is_coinbase"] else 'transfer',
                    amount,
                    self.version
                ])

                last_balance = current_balances.get(address, (0, Decimal('0')))[1]
                if (address, block_height) in new_balances:
                    last_balance = new_balances[(address, block_height)]
                new_balances[(address, block_height)] = last_balance + amount

        balances_list = [
            [address, block_height, balance]
            for (address, block_height), balance in sorted(new_balances.items())
            if balance >= 0  # Enforce positive balance constraint
        ]

        for balance in balances_list:
            balance.append(self.version)

        return changes, balances_list

    def index_transactions(self, transactions: List[Dict[str, Any]]):
        try:
            if not transactions:
                return

            addresses = self._collect_addresses(transactions)
            current_balances = self._get_latest_balances(addresses)

            balance_changes, new_balances = self._calculate_changes_and_balances(
                transactions, current_balances
            )

            if balance_changes:
                change_columns = ['address', 'block_height', 'tx_id', 'tx_index', 'event', 'balance_delta', '_version']
                self.client.insert('balance_changes', balance_changes, column_names=change_columns)

            if new_balances:
                balance_columns = ['address', 'block_height', 'balance', '_version']
                self.client.insert('balance_address_blocks', new_balances, column_names=balance_columns)

        except Exception as e:
            logger.error(f"Failed to index balances: {str(e)}")
            raise e

    def index_transactions_in_batches(self, transactions: List[Dict[str, Any]]):
        """Process transactions block by block in ascending order"""
        block_groups = {}
        for tx in transactions:
            block_height = tx["block_height"]
            if block_height not in block_groups:
                block_groups[block_height] = []
            block_groups[block_height].append(tx)

        for block_height in sorted(block_groups.keys()):
            block_txs = sorted(block_groups[block_height], key=lambda x: x["tx_index"])
            self.index_transactions(block_txs)
            logger.info(f"Processed block {block_height} with {len(block_txs)} transactions")