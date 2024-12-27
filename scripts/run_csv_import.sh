#!/bin/bash

python3 -m venv transaction-vout-indexer
source transaction-vout-indexer/bin/activate
pip install -r requirements.txt
cp .env transaction-vout-indexer/

cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
python3 node/transaction-vout-indexer/indexer.py --csvpath "$1" --threads "$2"

deactivate