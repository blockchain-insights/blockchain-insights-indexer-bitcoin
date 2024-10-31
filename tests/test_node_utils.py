import unittest
from node.node import BitcoinNode


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
