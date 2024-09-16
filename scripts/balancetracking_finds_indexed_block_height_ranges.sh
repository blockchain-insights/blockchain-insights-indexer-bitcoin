#!/bin/bash
cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
python3 models/balance_tracking/utils/find_indexed_block_height_ranges.py