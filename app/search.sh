#!/bin/bash

source .venv/bin/activate

# Setting Python paths for Spark driver and executor
export PYSPARK_DRIVER_PYTHON=$(which python)  # Set Python for the driver
export PYSPARK_PYTHON=./.venv/activate

source .venv/bin/activate

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
    --archives /app/.venv.tar.gz#.venv \
    /app/query.py "$1"