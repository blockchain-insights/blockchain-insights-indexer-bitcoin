#/bin/bash
cd "$(dirname "$0")/../"

docker run --rm -v .:/parser-data -v bitcoin-data:/home/bitcoin/.bitcoin --entrypoint '/bin/bash' ghcr.io/blockchain-insights/rusty_blockparser:latest -c "rusty-blockparser --start $BLOCK_PARSER_START_HEIGHT --end $BLOCK_PARSER_END_HEIGHT --blockchain-dir /home/bitcoin/.bitcoin/blocks csvdump / && mv /tx_out* /parser-data"