"""
{
    "tx_id": "272da16bce9a03c3aea2b294616e8e72e45dd29557938c10ed0848e62ad76018",
    "tx_index": 0,
    "timestamp": 1231865478,
    "block_height": 350,
    "is_coinbase": true,
    "in_total_amount": 0,
    "out_total_amount": 5000000000,
    "vins": [
          {
            "address": "13SxLs9mUbCyXrJ273nJLShaFq5dmDNiJM",
            "amount": 5000000000,
            "tx_id": "272da16bce9a03c3aea2b294616e8e72e45dd29557938c10ed0848e62ad76018"
        }
    ],
    "vouts": [
        {
            "address": "13SxLs9mUbCyXrJ273nJLShaFq5dmDNiJM",
            "amount": 5000000000,
            "tx_id": "272da16bce9a03c3aea2b294616e8e72e45dd29557938c10ed0848e62ad76018"
        }
    ]
}
"""

import mgp
import json

START_BLOCK_HEIGHT = 0


@mgp.transformation
def consume(messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    result_queries = []

    for i in range(messages.total_messages()):
        message = messages.message_at(i)
        payload_as_str = message.payload().decode("utf-8")
        tx = json.loads(payload_as_str)

        if tx["block_height"] < START_BLOCK_HEIGHT:
            continue

        # Create Transaction node
        result_queries.append(mgp.Record(
            query="""
            MERGE (t:Transaction {
                tx_id: $tx_id,
                timestamp: $timestamp,
                block_height: $block_height,
                is_coinbase: $is_coinbase,
                in_total_amount: $in_total_amount,
                out_total_amount: $out_total_amount
            })
            """,
            parameters={
                "tx_id": tx["tx_id"],
                "timestamp": tx["timestamp"],
                "block_height": tx["block_height"],
                "is_coinbase": tx["is_coinbase"],
                "in_total_amount": tx["in_total_amount"],
                "out_total_amount": tx["out_total_amount"]
            }
        ))

        # Create Address nodes and relationships for inputs (vins)
        for vin in tx["vins"]:
            # Create source Address node
            result_queries.append(mgp.Record(
                query="MERGE (a:Address {address: $address})",
                parameters={"address": vin["address"]}
            ))

            # Create relationship from Address to Transaction
            result_queries.append(mgp.Record(
                query="""
                MATCH (a:Address {address: $address})
                MATCH (t:Transaction {tx_id: $tx_id})
                MERGE (a)-[r:SENT {
                    amount: $amount
                }]->(t)
                """,
                parameters={
                    "address": vin["address"],
                    "tx_id": tx["tx_id"],
                    "amount": vin["amount"]
                }
            ))

        # Create Address nodes and relationships for outputs (vouts)
        for vout in tx["vouts"]:
            # Create destination Address node
            result_queries.append(mgp.Record(
                query="MERGE (a:Address {address: $address})",
                parameters={"address": vout["address"]}
            ))

            # Create relationship from Transaction to Address
            result_queries.append(mgp.Record(
                query="""
                MATCH (t:Transaction {tx_id: $tx_id})
                MATCH (a:Address {address: $address})
                MERGE (t)-[r:SENT {
                    amount: $amount
                }]->(a)
                """,
                parameters={
                    "tx_id": tx["tx_id"],
                    "address": vout["address"],
                    "amount": vout["amount"]
                }
            ))

        # Create direct Address-to-Address relationships
        for vin in tx["vins"]:
            for vout in tx["vouts"]:
                result_queries.append(mgp.Record(
                    query="""
                    MATCH (from:Address {address: $from_address})
                    MATCH (to:Address {address: $to_address})
                    MERGE (from)-[r:SENT {
                        amount: $amount,
                        block_height: $block_height
                    }]->(to)
                    """,
                    parameters={
                        "from_address": vin["address"],
                        "to_address": vout["address"],
                        "amount": vout["amount"],
                        "block_height": tx["block_height"]
                    }
                ))

    return result_queries