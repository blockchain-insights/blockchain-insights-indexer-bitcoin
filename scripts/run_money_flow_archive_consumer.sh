#!/bin/bash
python3 -m venv money_flow_archive_indexer
source money_flow_archive_indexer/bin/activate
pip install -r requirements.txt
cp .env money_flow_archive_indexer/

cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
python3 models/money_flow/money_flow_consumer.py --archive

deactivate
