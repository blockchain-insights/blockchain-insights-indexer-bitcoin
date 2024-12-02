## Money Flow Indexing Setup

There are 2 modes of operation for the Money Flow Indexing:
- realtime mode
- archive mode

Realtime mode is used for querying the data from last n years, typically 2; running advanced algorithms and analytics on low latency.
Archive mode is used for querying the data from the beginning of the blockchain, latency is higher, but the data is complete.
Each mode has its own setup and configuration. 
Both modes are using the same data source, which is the Redpanda stream.
The difference is in memgraph configuration and data stream configuration.
Realtime use IN_MEMORY_TRANSACTIONAL storage mode, while archive use ON_DISK_TRANSACTIONAL storage mode.
Before we start with proper part of indexing, we need to populate the redpanda stream with the data.

### Bitcoin Core setup
navigate to ops directory and run the following command
````
docker compose up -d bitcoin-core
````
wait until the bitcoin-core is fully synced with the network


### Redpanda setup
navigate to ops directory and run the following command
````
docker compose up -d redpanda redpanda-console redpanda-postgres
````


how to setup rendpanda, how to configure topic OR topic should be preconfigured during initial creation?
- prefonfigure topic to have be immutable, configure partitions based on block_height OR on ranges? (timestamps?) or each 50K blocks
- write manual how to configure memgraph stream, upload module, and start streaming
- add time log into memgraph module
- create indexes cypher query, add that into manual, no auto creation now


 

 

 

https://memgraph.com/docs/data-streams#kafka-and-redpanda

1) run block_stream
2) deploy block_stream_module into memgraph
3) craete redpanda stream in memgraph

    a) for memgraph archive mode

```
CREATE KAFKA STREAM block_stream
TOPICS transactions
TRANSFORM block_stream_module.consume
CONSUMER_GROUP block_stream_group
BATCH_SIZE 128
BATCH_INTERVAL 512
CONFIGS { "group.id": "transactions" }
BOOTSTRAP_SERVERS "redpanda:9092";
```

b) for memgraph live mode

```CREATE KAFKA STREAM block_stream
TOPICS transactions
TRANSFORM block_stream_module.consume
CONSUMER_GROUP block_stream_group
BATCH_SIZE 1024
BATCH_INTERVAL 100
CONFIGS { "group.id": "transactions" }
BOOTSTRAP_SERVERS "redpanda:9092";
```

4) verify created streams

```SHOW STREAMS;```

5) determinate starting offset for stream

```CALL mg.kafka_set_stream_offset('block_stream', {partition}, 0);```

| Partition | Block Height Range | Time Period |
|-----------|-------------------|--------------|
| 0 | 0 - 52,559 | Genesis - 2010 |
| 1 | 52,560 - 105,119 | 2010 - 2011 |
| 2 | 105,120 - 157,679 | 2011 - 2012 |
| 3 | 157,680 - 210,239 | 2012 - 2013 |
| 4 | 210,240 - 262,799 | 2013 - 2014 |
| 5 | 262,800 - 315,359 | 2014 - 2015 |
| 6 | 315,360 - 367,919 | 2015 - 2016 |
| 7 | 367,920 - 420,479 | 2016 - 2017 |
| 8 | 420,480 - 473,039 | 2017 - 2018 |
| 9 | 473,040 - 525,599 | 2018 - 2019 |
| 10 | 525,600 - 578,159 | 2019 - 2020 |
| 11 | 578,160 - 630,719 | 2020 - 2021 |
| 12 | 630,720 - 683,279 | 2021 - 2022 |
| 13 | 683,280 - 735,839 | 2022 - 2023 |
| 14 | 735,840 - 788,399 | 2023 - 2024 |
| 15 | 788,400 - 840,959 | 2024 - 2025 |
| 16 | 840,960 - 893,519 | 2025 - 2026 |
| 17 | 893,520 - 946,079 | 2026 - 2027 |
| 18 | 946,080 - 998,639 | 2027 - 2028 |
| 19 | 998,640 - 1,051,199 | 2028 - 2029 |
| 20 | 1,051,200 - 1,103,759 | 2029 - 2030 |
| 21 | 1,103,760 - 1,156,319 | 2030 - 2031 |
| 22 | 1,156,320 - 1,208,879 | 2031 - 2032 |
| 23 | 1,208,880 - 1,261,439 | 2032 - 2033 |
| 24 | 1,261,440 - 1,313,999 | 2033 - 2034 |
| 25 | 1,313,999 - 1,366,559 | 2034 - 2035 |
| 26 | 1,366,560 - 1,419,119 | 2035 - 2036 |
| 27 | 1,419,120 - 1,471,679 | 2036 - 2037 |
| 28 | 1,471,680 - 1,524,239 | 2037 - 2038 |
| 29 | 1,524,240 - 1,576,799 | 2038 - 2039 |
| 30 | 1,576,800+ | 2039+ |


CALL mg.kafka_set_stream_offset('block_stream', 1, 1000);

SHOW STREAMS;

START STREAM streamName;
STOP STREAM streamName;
DROP STREAM streamName;

