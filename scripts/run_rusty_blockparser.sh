#/bin/bash
cd "$(dirname "$0")/../"

docker run --rm -v "$(pwd)":/parser-data -v blockchain-bitcoin-data:/home/bitcoin/.bitcoin ghcr.io/blockchain-insights/rusty_blockparser:latest --start $BLOCK_PARSER_START_HEIGHT --end $BLOCK_PARSER_END_HEIGHT --blockchain-dir /home/bitcoin/.bitcoin/blocks csvdump /parser-data/tx_vout



