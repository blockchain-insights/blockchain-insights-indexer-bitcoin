from decimal import Decimal

from bitcoin.core import b2lx
from loguru import logger
from bitcoin.rpc import Proxy, RawProxy, InvalidAddressOrKeyError
import json
import http.client
import base64

from .abstract_node import Node
from .node_utils import (
    pubkey_to_address,
    construct_redeem_script,
    hash_redeem_script,
    create_p2sh_address,
    Transaction, SATOSHI, VOUT, VIN, derive_address
)


class ExtendedProxy(Proxy):
    def getblock(self, block_hash, verbosity=2):
        """Get block <block_hash> with specified verbosity

        Args:
            block_hash: The block hash
            verbosity: 0 for raw hex, 1 for json object, 2 for json object with transaction data
        """
        try:
            block_hash = b2lx(block_hash)
        except TypeError:
            raise TypeError('%s.getblock(): block_hash must be bytes; got %r instance' %
                            (self.__class__.__name__, block_hash.__class__))
        try:
            r = self._call('getblock', block_hash, verbosity)
            return r
        except InvalidAddressOrKeyError as ex:
            raise IndexError('%s.getblock(): %s (%d)' %
                             (self.__class__.__name__, ex.error['message'], ex.error['code']))

    def batch_request(self, commands):
        try:
            # Build authorization header
            auth = base64.b64encode(
                f"{self._BaseProxy__url.username}:{self._BaseProxy__url.password}".encode()
            ).decode()

            # Prepare batch request
            batch = []
            for i, cmd in enumerate(commands):
                method, *params = cmd
                batch.append({
                    "method": method,
                    "params": params,
                    "jsonrpc": "2.0",
                    "id": i,
                })

            # Create HTTP connection
            conn = http.client.HTTPConnection(
                self._BaseProxy__url.hostname,
                self._BaseProxy__url.port
            )

            headers = {
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/json",
            }

            # Make the request
            conn.request(
                "POST",
                "/",
                json.dumps(batch),
                headers
            )

            # Get and parse response
            response = conn.getresponse()
            result = json.loads(response.read().decode())
            conn.close()

            # Handle response
            if isinstance(result, list):
                for r in result:
                    if "error" in r and r["error"] is not None:
                        raise Exception(f"Error in command {r['id']}: {r['error']}")
                return [r["result"] for r in result]
            else:
                if result.get("error"):
                    raise Exception(f"Error in batch request: {result['error']}")
                return [result.get("result")]

        except Exception as e:
            logger.error(f"Batch request failed: {str(e)}")
            raise


class BitcoinNode(Node):
    def __init__(self, node_rpc_url: str):
        self.node_rpc_url = node_rpc_url

    def get_current_block_height(self):
        proxy = Proxy(service_url=self.node_rpc_url)
        try:
            result = proxy.getblockcount()
            return result
        except Exception as e:
            logger.error(f"RPC Provider with Error", error=str(e))
        finally:
            proxy.close()

    def get_block_by_height(self, block_height):
        proxy = ExtendedProxy(service_url=self.node_rpc_url)
        try:
            block_hash = proxy.getblockhash(block_height)
            result = proxy.getblock(block_hash)
            return result
        except Exception as e:
            logger.error(f"RPC Provider with Error", error=str(e))
        finally:
            proxy.close()

    def get_address_and_amount_by_txn_id_and_vout_id(self, txn_id: str, vout_id: str):
        result = self.get_addresses_and_amounts_by_txouts([(txn_id, vout_id)])
        return result.get((txn_id, vout_id), (f"unknown-{txn_id}", 0))

    def get_txn_data_by_id(self, txn_id: str):
        proxy = Proxy(service_url=self.node_rpc_url)
        try:
            result = proxy.getrawtransaction(txn_id, 1)
            return result
        except Exception as e:
            logger.error(f"RPC Provider with Error", error=str(e))
            return None
        finally:
            proxy.close()

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
        proxy = ExtendedProxy(service_url=self.node_rpc_url)
        try:
            commands = [[
                "getblockhash",
                height
            ] for height in range(start_height, end_height + 1)]

            block_hashes = proxy.batch_request(commands)

            commands = [[
                "getblock",
                block_hash,
                2
            ] for block_hash in block_hashes]

            result = proxy.batch_request(commands)
            return result
        except Exception as e:
            logger.error(f"RPC Provider with Error", error=str(e))
            return []
        finally:
            proxy.close()

    def get_transactions_by_ids(self, tx_ids: list):
        proxy = ExtendedProxy(service_url=self.node_rpc_url)
        try:
            commands = [[
                "getrawtransaction",
                tx_id,
                1,
            ] for tx_id in tx_ids]

            result = proxy.batch_request(commands)
            return result
        except Exception as e:
            logger.error(f"RPC Provider with Error", error=str(e))
            return []
        finally:
            proxy.close()

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
                missing_txouts.append((txn_id, vout_id))

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
