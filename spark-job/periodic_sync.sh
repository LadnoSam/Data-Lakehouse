#!/bin/bash

set -e

SYNC_INTERVAL_SECONDS=${SYNC_INTERVAL_SECONDS:-300}

echo "[Service] Started interval=${SYNC_INTERVAL_SECONDS}s"

while true; do

    echo ""
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SYNC START"

    # MongoDB
    python3 - <<'PY'
from pymongo import MongoClient
import os

client = MongoClient(os.environ["MONGO_URL"])
collection = client["crypto"]["ethereum_prices"]

latest = collection.find_one({}, sort=[("timestamp", -1)])

if latest:
    print(
        f"[MongoDB] OK "
        f"latest={latest.get('timestamp')} "
        f"price=${latest.get('price_usd')} "
        f"change={latest.get('change_24h')}%"
    )
else:
    print("[MongoDB] ERROR no documents")
    raise SystemExit(1)

client.close()
PY

    # MongoDB -> Iceberg
    echo "[Sync] START MongoDB -> Iceberg"

    /opt/spark/bin/spark-submit \
        --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        /opt/spark-job/eth_to_iceberg.py \
        >/tmp/eth_to_iceberg.log 2>&1

    echo "[Sync] OK MongoDB -> Iceberg"

    # Iceberg verification
    /opt/spark/bin/spark-submit \
        --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        /opt/spark-job/check_latest.py

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SYNC DONE"
    echo "[Service] Next sync in ${SYNC_INTERVAL_SECONDS}s"

    sleep "${SYNC_INTERVAL_SECONDS}"

done

