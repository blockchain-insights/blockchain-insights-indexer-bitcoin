#!/bin/bash
python3 -m venv venv_balance_tracking_indexer
source venv_balance_tracking_indexer/bin/activate
pip install -r requirements.txt
cp .env venv_balance_tracking_indexer/

cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
python3 models/balance_tracking/block_stream_consumer.py

deactivate