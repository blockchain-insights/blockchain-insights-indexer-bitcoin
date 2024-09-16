#!/bin/bash
cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
python3 models/funds_flow/utils/sync_block_height_range_cache.py