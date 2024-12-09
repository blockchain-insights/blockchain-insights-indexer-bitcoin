#!/bin/bash
python3 -m venv block_stream
source block_stream/bin/activate
pip install -r requirements.txt
cp .env block_stream/

cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
python3 models/block_stream.py

deactivate

