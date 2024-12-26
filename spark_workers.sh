#!/bin/bash

# Wait for the Spark Master to be ready
until (curl -s spark-master:8080 > /dev/null) && (curl -s 127.0.0.1:8080 > /dev/null); do
  echo "Waiting for Spark Master to be ready..."
  sleep 5
done

kill -9 $(ps aux | grep webui-port| awk '{print $2}')

# Start the worker
exec start-worker.sh $SPARK_MASTER_URL --cores $SPARK_WORKER_CORES --memory $SPARK_WORKER_MEMORY
