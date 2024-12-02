# Bitcoin Indexer

## Hardware Requirements
- MemGraph (IN_MEMORY_TRANSACTIONAL): 2TB+ RAM, 32+ CPU cores, ~7TB+ SSD/nvme storage
- MemGraph (ON_DISK_TRANSACTIONAL): 1TB+ RAM, 16+ CPU cores, ~7TB+ SSD/nvme storage
- Redpanda: 0.256TB+ RAM, 8+ CPU cores, ~7TB+ SSD/nvme storage
- Bitcoin full node: 1TB+ SSD/nvme storage, 8+ CPU cores, 64GB+ RAM
- Indexer: 4+ CPU, 2 GB RAM

## System Configuration

```diff
- Most users need only what is explained in this documentation. Editing the docker-compose files and the optional variables may create problems and is for advanced users only!
```

### Setup

- Make sure you have `pm2` installed from the miner [documentation](https://github.com/blockchain-insights/blockchain-insights-subnet/blob/main/MINER_SETUP.md#prerequisites)

- Clone this repository:
    ```bash
    git clone https://github.com/blockchain-insights/blockchain-insights-indexer-bitcoin.git
    ```
- Navigate to ```blockchain-insights-indexer-bitcoin/ops``` and copy the example ```.env``` file:
    ```bash
    cp .env.example .env
    ```

### Bitcoin node, Memgraph Real Time, Memgraph Archive, Timescale and Indexer
 
- **Running Bitcoin node**

    Open the ```.env``` file:
    ```bash
    nano .env
    ```
    Set the required variables in the ```.env``` file and save it:
    ```ini
    RPC_USER=your_secret_user_name
    RPC_PASSWORD=your_secret_password
    ```
    
    **Optional ```.env``` variables with their defaults. Add them to your ```.env``` file ONLY if you are not satisfied with the defaults:**
    ```ini
    RPC_ALLOW_IP=172.16.0.0/12
    RPC_BIND=0.0.0.0
    MAX_CONNECTIONS=512
    ```

    Start the Bitcoin node:
    ```bash
    docker compose up -d bitcoin-core
    ```

- **Running Real time instance of Memgraph**

    Open the ```.env``` file:
    ```
    nano .env
    ```

    Set the required variables in the ```.env``` file and save it:
    ```ini
    GRAPH_DB_USER=your_secret_user_name
    GRAPH_DB_PASSWORD=your_secret_password
    ```
  
    Configure Max Map Count:
    ```bash
    # For 1TB RAM
    echo "vm.max_map_count=8388608" | sudo tee -a /etc/sysctl.conf
    
    # For 1.5TB RAM
    echo "vm.max_map_count=12582912" | sudo tee -a /etc/sysctl.conf
    
    # For 2TB RAM
    echo "vm.max_map_count=16777216" | sudo tee -a /etc/sysctl.conf
    
    # For 2.5TB RAM
    echo "vm.max_map_count=20971520" | sudo tee -a /etc/sysctl.conf
    
    # For 3TB RAM
    echo "vm.max_map_count=25165824" | sudo tee -a /etc/sysctl.conf
    
    # For 3.5TB RAM
    echo "vm.max_map_count=29360128" | sudo tee -a /etc/sysctl.conf
    
    # For 4TB RAM
    echo "vm.max_map_count=33554432" | sudo tee -a /etc/sysctl.conf

    sudo sysctl -p
    ```
  
    Start the Memgraph
    ```
    docker compose up -d memgraph
    ```
  
- **Running Archive instance of Memgraph**

    Open the ```.env``` file:
    ```
    nano .env
    ```

    Set the required variables in the ```.env``` file and save it:
    ```ini
    GRAPH_DB_USER=your_secret_user_name
    GRAPH_DB_PASSWORD=your_secret_password
    ``` 
  
    Start the Memgraph
    ```
    docker compose up -d memgraph-archive
    ```

- **Running Postgres with TimescaleDB extension**

    Open the ```.env``` file:
    ```
    nano .env
    ```

    Set the required variables in the ```.env``` file and save it:
    ```ini
    POSTGRES_USER=your_secret_user_name
    POSTGRES_PASSWORD=your_secret_password
    ```

    **Optional ```.env``` variables with their defaults. Add them to your ```.env``` file ONLY if you are not satisfied with the defaults:**
    ```ini
    POSTGRES_DB=miner
    ```

    Start the Postgres
    ```
    docker compose up -d postgres
    ```

- **Running Money Flow Indexer**

    Money Flow Indexing is a slow process which can be accelerated by first generating pickle files for some of the blocks.
    For recent blocks, pickle files with at least 100000 size help with indexing speed.
    Below is an example procedure which does the following:
     - Generate pickle files for the recent blocks
     - Index them and start reverse indexing to block 1
     - When reverse indexing is done, you start forward indexing

    You can experiment with this process - the number of blocks, the reverse/forward indexing, smart indexing etc. Generating pickle files makes indexing faster, but takes a lot of memory, so keep an eye on the memory usage. Note, you need a local Bitcoin node running and **synced** before starting.

    **0. Navigate back to the root of the repository:**
    ```bash
    cd ..
    ```

    **1. Run block parser to generate tx_out csv file:**
    ```bash
    BLOCK_PARSER_START_HEIGHT=700000 BLOCK_PARSER_END_HEIGHT=830000 ./scripts/run_indexer_bitcoin_block_parser.sh
    ```
    You can find `tx_out-{BLOCK_PARSER_START_HEIGHT}-{BLOCK_PARSER_END_HEIGHT}.csv` generated in the current directory. Run `ls` to see the generated files.

    **2. Run vout hashtable builder to generate pickle file from csv:**
    ```bash
    CSV_FILE=tx_out-700000-830000.csv TARGET_PATH=700000-830000.pkl ./scripts/run_indexer_bitcoin_vout_hashtable_builder.sh
    ```
    You can find `{BLOCK_PARSER_START_HEIGHT}-{BLOCK_PARSER_END_HEIGHT}.pkl` generated in the current directory. Run `ls` to see the generated files.
    
    **3. Start reverse indexing:**

    In REVERSE ORDER, START_BLOCK should be greater than END_BLOCK. You can specify multiple pickle files separated by comma. 
    ```bash
    BITCOIN_INDEXER_IN_REVERSE_ORDER=1 BITCOIN_INDEXER_START_BLOCK_HEIGHT=830000 BITCOIN_INDEXER_END_BLOCK_HEIGHT=0 BITCOIN_V2_TX_OUT_HASHMAP_PICKLES=700000-830000.pkl pm2 start ./scripts/run_block_stream.sh --name reverse-indexer
    ```

    You can monitor the progress using the following command:
    ```bash
    ./script/funds_flow_finds_indexed_block_height_ranges.sh
    ```

    **4. Start forward indexing:**

    When the reverse indexer is ready, delete the process:

    ```bash
    pm2 delete reverse-indexer
    ```

    You can monitor the progress using the following command:
    ```bash
    ./script/funds_flow_finds_indexed_block_height_ranges.sh
    ```

    **5. (Optional) Start smart indexing (run forward and reverse indexer simultaneously):**

    In smart mode, the indexer starts at START_HEIGHT and index forward to the latest block. If it reaches the latest block, it runs reverse indexing while waiting. If the new block is mined, it indexes the new block and runs reverse indexing again. If it finished reverse indexing, it just indexes latest blocks in real-time.

    If any of the indexers is running, delete the process:

    ```bash
    pm2 delete reverse-indexer
    pm2 delete forward-indexer
    ```

    Start the smart indexer:
    ```bash
    BITCOIN_INDEXER_SMART_MODE=1 BITCOIN_INDEXER_START_BLOCK_HEIGHT=830000 pm2 start ./scripts/run_block_stream.sh --name smart-indexer
    ```

    You can monitor the progress using the following command:
    ```bash
    ./script/funds_flow_finds_indexed_block_height_ranges.sh
    ```

- **Running Balance Tracking Indexer**

    Balance Tracking Indexer also takes long and requires loading pickle files like Money Flow Indexer.

    Start the balance tracking indexer:

    ```bash
    pm2 start ./scripts/run_indexer_bitcoin_balance_tracking.sh --name balance-tracking-indexer
    ```

    You can monitor the progress using the following command:
    ```bash
    ./scripts/balancetracking_finds_indexed_block_height_ranges.sh
    ```

- **Running Miner**

    When indexers are in sync, you are ready to [start the miners](https://github.com/blockchain-insights/blockchain-insights-subnet/blob/main/MINER_SETUP.md).