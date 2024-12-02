import unittest
from node.node import BitcoinNode
from bitcoin.rpc import Proxy, RawProxy
import json
import http.client
import base64


def batch_request(proxy, commands):
    auth = base64.b64encode(
        f"{proxy._BaseProxy__url.username}:{proxy._BaseProxy__url.password}".encode()
    ).decode()

    batch = []
    for i, cmd in enumerate(commands):
        method, *params = cmd
        batch.append({
            "method": method,
            "params": params,
            "jsonrpc": "2.0",
            "id": i,
        })

    conn = http.client.HTTPConnection(proxy._BaseProxy__url.hostname, proxy._BaseProxy__url.port)
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
    }

    conn.request(
        "POST",
        "/",
        json.dumps(batch),
        headers
    )

    response = conn.getresponse()
    result = json.loads(response.read().decode())
    conn.close()

    for r in result:
        if "error" in r and r["error"] is not None:
            raise Exception(f"Error in command {r['id']}: {r['error']}")

    return [r["result"] for r in result]


class TestNodeCommandExecution(unittest.TestCase):

    def test_new_lib(self):
        # Connect to node
        proxy = Proxy(service_url="http://daxtohujek446464:lubosztezhujek3446457@localhost:8332")

        commands = [
            ["getrawtransaction", "031a973434d49f03e0761bfcc9bdadd2d24ba9db37c45a6156d454921b65c48d", 1],
        ]
        results = batch_request(proxy, commands)

        print(results)


class TestNodeUtils(unittest.TestCase):
    def test_process_in_memory_txn_for_indexing(self):
        txn_id = "031a973434d49f03e0761bfcc9bdadd2d24ba9db37c45a6156d454921b65c48d"
        node = BitcoinNode()
        
        txn_data = node.get_txn_data_by_id(txn_id)
        tx = node.create_in_memory_txn(txn_data)
        result = node.process_in_memory_txn_for_indexing(tx)

        # Check the result
        expected_result = ({'1G8v6gMAKmChVib2CdRD5fgt7MZuQBNv6J': 70000280000}, {'15NfoecUAFD8s2H7rYTXSY6w4VWYuJbiPZ': 50000280000, '1GR2iC2NzXxShB2QWejcof1DqAvAMR3rpC': 20000000000}, ['1G8v6gMAKmChVib2CdRD5fgt7MZuQBNv6J'], ['15NfoecUAFD8s2H7rYTXSY6w4VWYuJbiPZ','1GR2iC2NzXxShB2QWejcof1DqAvAMR3rpC'], 70000280000, 70000280000)
        self.assertEqual(result, expected_result)

    def test_coinbase_tx(self):
        txn_id = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098"
        node = BitcoinNode()

        txn_data = node.get_txn_data_by_id(txn_id)
        tx = node.create_in_memory_txn(txn_data)
        result = node.process_in_memory_txn_for_indexing(tx)

        expected_result = ({}, {'12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX': 5000000000}, [], ['12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX'], 0, 5000000000)
        self.assertEqual(result, expected_result)



if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    unittest.main()
