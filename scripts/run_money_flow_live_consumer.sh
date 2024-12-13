#!/bin/bash
python3 -m venv money_flow_live_indexer
source money_flow_live_indexer/bin/activate
pip install -r requirements.txt
cp .env money_flow_live_indexer/

cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
python3 models/money_flow/block_stream_consumer.py

deactivate
