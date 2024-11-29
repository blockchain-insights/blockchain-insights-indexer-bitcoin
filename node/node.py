from decimal import Decimal
from bitcoinrpc.authproxy import AuthServiceProxy
from loguru import logger

from .abstract_node import Node
from .node_utils import (
    pubkey_to_address,
    construct_redeem_script,
    hash_redeem_script,
    create_p2sh_address,
    Transaction, SATOSHI, VOUT, VIN, derive_address
)

from .node_utils import initialize_tx_out_hash_table, get_tx_out_hash_table_sub_keys

import pickle
import time
import os


class BitcoinNode(Node):
    def __init__(self, node_rpc_url: str = None):
        self.tx_out_hash_table = initialize_tx_out_hash_table()
        if node_rpc_url is None:
            self.node_rpc_url = (
                    os.environ.get("BITCOIN_NODE_RPC_URL")
                    or "http://bitcoinrpc:rpcpassword@127.0.0.1:8332"
            )
        else:
            self.node_rpc_url = node_rpc_url

    def get_current_block_height(self):
        rpc_connection = AuthServiceProxy(self.node_rpc_url)
        try:
            return rpc_connection.getblockcount()
        except Exception as e:
            logger.error(f"RPC Provider with Error",
                         error={'exception_type': e.__class__.__name__, 'exception_message': str(e),
                                'exception_args': e.args})
        finally:
            rpc_connection._AuthServiceProxy__conn.close()  # Close the connection

    def get_block_by_height(self, block_height):
        rpc_connection = AuthServiceProxy(self.node_rpc_url)
        try:
            block_hash = rpc_connection.getblockhash(block_height)
            return rpc_connection.getblock(block_hash, 2)
        except Exception as e:
            logger.error(f"RPC Provider with Error",
                         error={'exception_type': e.__class__.__name__, 'exception_message': str(e),
                                'exception_args': e.args})
        finally:
            rpc_connection._AuthServiceProxy__conn.close()  # Close the connection

    def get_address_and_amount_by_txn_id_and_vout_id(self, txn_id: str, vout_id: str):
        """Single transaction output lookup, now using the batch method internally"""
        result = self.get_addresses_and_amounts_by_txouts([(txn_id, vout_id)])
        return result.get((txn_id, vout_id), (f"unknown-{txn_id}", 0))

    def get_txn_data_by_id(self, txn_id: str):
        try:
            rpc_connection = AuthServiceProxy(self.node_rpc_url)
            return rpc_connection.getrawtransaction(txn_id, 1)
        except Exception as e:
            return None

    @staticmethod
    def create_in_memory_txn(tx_data):
        tx = Transaction(
            tx_id=tx_data.get('txid'),
            block_height=0,
            timestamp=0,
            fee_satoshi=0
        )

        for vin_data in tx_data["vin"]:
            vin = VIN(
                tx_id=vin_data.get("txid", 0),
                vin_id=vin_data.get("sequence", 0),
                vout_id=vin_data.get("vout", 0),
                script_sig=vin_data.get("scriptSig", {}).get("asm", ""),
                sequence=vin_data.get("sequence", 0),
            )
            tx.vins.append(vin)
            tx.is_coinbase = "coinbase" in vin_data

        for vout_data in tx_data["vout"]:
            script_type = vout_data["scriptPubKey"].get("type", "")
            if "nonstandard" in script_type or script_type == "nulldata":
                continue

            value_satoshi = int(Decimal(vout_data["value"]) * SATOSHI)
            n = vout_data["n"]
            script_pub_key_asm = vout_data["scriptPubKey"].get("asm", "")

            address = vout_data["scriptPubKey"].get("address", "")
            if not address:
                addresses = vout_data["scriptPubKey"].get("addresses", [])
                if addresses:
                    address = addresses[0]
                elif "OP_CHECKSIG" in script_pub_key_asm:
                    pubkey = script_pub_key_asm.split()[0]
                    address = pubkey_to_address(pubkey)
                elif "OP_CHECKMULTISIG" in script_pub_key_asm:
                    pubkeys = script_pub_key_asm.split()[1:-2]
                    m = int(script_pub_key_asm.split()[0])
                    redeem_script = construct_redeem_script(pubkeys, m)
                    hashed_script = hash_redeem_script(redeem_script)
                    address = create_p2sh_address(hashed_script)
                else:
                    raise Exception(
                        f"Unknown address type: {vout_data['scriptPubKey']}"
                    )

            vout = VOUT(
                vout_id=n,
                value_satoshi=value_satoshi,
                script_pub_key=script_pub_key_asm,
                is_spent=False,
                address=address,
            )
            tx.vouts.append(vout)

        return tx

    def process_in_memory_txn_for_indexing(self, tx):
        input_amounts = {}  # input amounts by address in satoshi
        output_amounts = {}  # output amounts by address in satoshi

        for vin in tx.vins:
            if vin.tx_id == 0:
                continue
            address, amount = self.get_address_and_amount_by_txn_id_and_vout_id(vin.tx_id, str(vin.vout_id))
            input_amounts[address] = input_amounts.get(address, 0) + amount

        for vout in tx.vouts:
            amount = vout.value_satoshi
            address = vout.address or f"unknown-{tx.tx_id}"
            output_amounts[address] = output_amounts.get(address, 0) + amount

        for address in input_amounts:
            if address in output_amounts:
                diff = input_amounts[address] - output_amounts[address]
                if diff > 0:
                    input_amounts[address] = diff
                    output_amounts[address] = 0
                elif diff < 0:
                    output_amounts[address] = -diff
                    input_amounts[address] = 0
                else:
                    input_amounts[address] = 0
                    output_amounts[address] = 0

        input_addresses = [address for address, amount in input_amounts.items() if amount != 0]
        output_addresses = [address for address, amount in output_amounts.items() if amount != 0]

        in_total_amount = sum(input_amounts.values())
        out_total_amount = sum(output_amounts.values())

        return input_amounts, output_amounts, input_addresses, output_addresses, in_total_amount, out_total_amount

    def get_blocks_by_height_range(self, start_height: int, end_height: int):
        """Batch fetch blocks in a range"""
        rpc_connection = AuthServiceProxy(self.node_rpc_url)
        try:
            # Block hash batch
            commands = [[
                "getblockhash",
                height
            ] for height in range(start_height, end_height + 1)]

            block_hashes = rpc_connection.batch_(commands)

            # Block batch
            commands = [[
                "getblock",
                block_hash,
                2
            ] for block_hash in block_hashes]

            return rpc_connection.batch_(commands)
        except Exception as e:
            logger.error(f"RPC provider with error",
                         error={
                             'exception_type': e.__class__.__name__,
                             'exception_message': str(e),
                             'exception_args': e.args
                         })
            return []
        finally:
            rpc_connection._AuthServiceProxy__conn.close()

    def get_transactions_by_ids(self, tx_ids: list):
        """Batch fetch txs by id"""
        rpc_connection = AuthServiceProxy(self.node_rpc_url)
        try:
            commands = [[
                "getrawtransaction",
                tx_id,
                1
            ] for tx_id in tx_ids]

            return rpc_connection.batch_(commands)
        except Exception as e:
            logger.error(f"RPC provider with error",
                         error = {
                             'exception_type': e.__class__.__name__,
                             'exception_message': str(e),
                             'exception_args': e.args
                         })
            return []
        finally:
            rpc_connection._AuthServiceProxy__conn.close()

    def get_addresses_and_amounts_by_txouts(self, txouts: list):
        """
        Batch fetch addresses and amounts for multiple txouts
        txouts should be a list of (txn_id, vout_id) tuples
        :param txouts:
        :return:
        """

        # First check hash table for all entries
        results = {}
        missing_txouts = []

        for txn_id, vout_id in txouts:
            if (txn_id, vout_id) in self.tx_out_hash_table[txn_id[:3]]:
                address, amount = self.tx_out_hash_table[txn_id[:3]][(txn_id, vout_id)]
                results[(txn_id, vout_id)] = (address, int(amount))
            else:
                missing_txouts.append((txn_id, vout_id))

                # If we have missing entries, fetch them in batch
        if missing_txouts:
            unique_tx_ids = list(set(tx_id for tx_id, _ in missing_txouts))
            tx_data_list = self.get_transactions_by_ids(unique_tx_ids)

            # Create a mapping of txid to transaction data
            tx_map = {tx['txid']: tx for tx in tx_data_list if tx}

            for txn_id, vout_id in missing_txouts:
                if txn_id in tx_map:
                    tx_data = tx_map[txn_id]
                    vout = next((x for x in tx_data['vout'] if str(x['n']) == str(vout_id)), None)

                    if vout:
                        amount = int(vout['value'] * 100000000)
                        address = derive_address(vout["scriptPubKey"],
                                                 vout["scriptPubKey"].get("asm", ""))
                    else:
                        address = f"unknown-{txn_id}"
                        amount = 0
                else:
                    address = f"unknown-{txn_id}"
                    amount = 0

                results[(txn_id, vout_id)] = (address, amount)

        return results
