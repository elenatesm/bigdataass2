#!/bin/bash

source .venv/bin/activate

echo "[INFO] Creating Cassandra schema using app.py..."
python3 /app/app.py || { echo "[ERROR] Failed to run app.py"; exit 1; }

# 1. Clean up previous outputs
for PIPELINE_PATH in /tmp/index/tf /tmp/index/df /tmp/index/doc; do
    if hdfs dfs -test -d "$PIPELINE_PATH"; then
        echo "[INFO] Removing $PIPELINE_PATH..."
        hdfs dfs -rm -r "$PIPELINE_PATH"
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to remove $PIPELINE_PATH" >&2
            exit 1
        fi
    else
        echo "[INFO] $PIPELINE_PATH does not exist, skipping removal"
    fi
done

# 2. Zip python libs to pass as a file to Jar
echo "Packing python libs"
zip -r python_libs.zip /app/.venv/lib/python*/site-packages/cassandra*

# 3. Pipeline 1: Term Frequency
echo "[INFO] Starting Pipeline 1: Term Frequency"

mapred streaming \
    -D stream.map.output.field.separator='\t' \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keypartitioner.options=-k1,1 \
    -D mapreduce.partition.keycomparator.options='-k1,1 -k2,2' \
    -files "/app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py,python_libs.zip" \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input /index/data \
    -output /tmp/index/tf \
    -numReduceTasks 5


if [ $? -ne 0 ]; then
    echo "[ERROR] Pipeline 1 failed!" >&2
    exit 1
fi


# 4. Pipeline 2: Document Frequency
echo "[INFO] Starting Pipeline 2: Documet Frequency"

mapred streaming \
    -files "/app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py,python_libs.zip" \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -input /index/data \
    -output /tmp/index/df \
    -numReduceTasks 5


if [ $? -ne 0 ]; then
    echo "[ERROR] Pipeline 2 failed!" >&2
    exit 1
fi

# 5. Pipeline 3: Document Length
echo "[INFO] Starting Pipeline 3: Document Information"

mapred streaming \
    -files "/app/mapreduce/mapper3.py,/app/mapreduce/reducer3.py,python_libs.zip" \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -input /index/data \
    -output /tmp/index/doc \
    -numReduceTasks 5

if [ $? -ne 0 ]; then
    echo "[ERROR] Pipeline 3 failed!" >&2
    exit 1
fi

echo "[INFO] Indexing completed successfully!"
