#!/bin/bash
cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
python3 models/money_flow/block_stream_consumer.py --archive