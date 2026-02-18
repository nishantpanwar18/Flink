#!/bin/bash

echo "=========================================="
echo "Submit PyFlink Job to Cluster"
echo "=========================================="
echo ""

# Check if job file is provided
if [ -z "$1" ]; then
    echo "Usage: ./submit_to_flink.sh <job_file.py>"
    echo ""
    echo "Examples:"
    echo "  ./submit_to_flink.sh cluster_jobs/app_usage_cluster.py"
    echo ""
    exit 1
fi

JOB_FILE=$1

# Check if file exists
if [ ! -f "$JOB_FILE" ]; then
    echo "❌ Error: File $JOB_FILE not found"
    exit 1
fi

echo "📦 Job file: $JOB_FILE"
echo ""

# Copy job file and dependencies to JobManager container
echo "📤 Copying files to Flink JobManager..."
docker cp $JOB_FILE flink-jobmanager:/tmp/job.py
docker cp lib/flink-sql-connector-kafka-3.0.2-1.18.jar flink-jobmanager:/opt/flink/lib/

echo "✅ Files copied"
echo ""

# Submit job to Flink cluster
echo "🚀 Submitting job to Flink cluster..."
docker exec flink-jobmanager /opt/flink/bin/flink run \
    --python /tmp/job.py \
    --jarfile /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar \
    --detached

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Job submitted successfully!"
    echo ""
    echo "View job in Flink UI:"
    echo "  http://localhost:8081"
    echo ""
else
    echo ""
    echo "❌ Job submission failed"
    echo "Check logs: docker logs flink-jobmanager"
    exit 1
fi
