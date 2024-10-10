#!/bin/bash
cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
source ops/.env
export DB_CONNECTION_STRING="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/$POSTGRES_DB"
python3 models/balance_tracking/indexer.py