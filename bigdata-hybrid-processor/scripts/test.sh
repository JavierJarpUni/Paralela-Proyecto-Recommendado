#!/bin/bash
# test.sh

API_URL="https://us-central1-flash-freehold-454723-c7.cloudfunctions.net/bigdata-api-gateway"

echo "Testing BigData Hybrid Application..."

# 1. Test data generation
echo "Generating test data..."
TEST_DATA=$(python3 -c "
import json
import random
data = [random.uniform(-100, 100) for _ in range(1000)]
print(json.dumps(data))
")

# 2. Start processing
echo "Starting processing..."
JOB_RESPONSE=$(curl -s -X POST "$API_URL/process" \
  -H "Content-Type: application/json" \
  -d "{\"data\": $TEST_DATA}")

JOB_ID=$(echo $JOB_RESPONSE | jq -r '.job_id')
echo "Job ID: $JOB_ID"

if [ "$JOB_ID" = "null" ]; then
  echo "Failed to start processing"
  echo $JOB_RESPONSE
  exit 1
fi

# 3. Monitor progress
echo "👀 Monitoring progress..."
while true; do
  STATUS_RESPONSE=$(curl -s "$API_URL/status/$JOB_ID")
  STATUS=$(echo $STATUS_RESPONSE | jq -r '.status')
  PROGRESS=$(echo $STATUS_RESPONSE | jq -r '.progress_percentage')
  
  echo "📈 Status: $STATUS, Progress: $PROGRESS%"
  
  if [ "$STATUS" = "completed" ]; then
    break
  elif [ "$STATUS" = "failed" ]; then
    echo "Processing failed"
    echo $STATUS_RESPONSE
    exit 1
  fi
  
  sleep 10
done

# 4. Get results
echo "Retrieving results..."
RESULTS_RESPONSE=$(curl -s "$API_URL/results/$JOB_ID")
echo $RESULTS_RESPONSE | jq .

# 5. Performance analysis
echo "Performance Analysis:"
RDD_TIME=$(echo $RESULTS_RESPONSE | jq -r '.results.spark_rdd.processing_time_ms')
DF_TIME=$(echo $RESULTS_RESPONSE | jq -r '.results.spark_dataframe.processing_time_ms')

if [ "$RDD_TIME" != "null" ] && [ "$DF_TIME" != "null" ]; then
  echo "⚡ RDD Processing Time: ${RDD_TIME}ms"
  echo "⚡ DataFrame Processing Time: ${DF_TIME}ms"
  
  if [ $DF_TIME -lt $RDD_TIME ]; then
    SPEEDUP=$(echo "scale=2; $RDD_TIME / $DF_TIME" | bc)
    echo "DataFrame is ${SPEEDUP}x faster than RDD"
  else
    SPEEDUP=$(echo "scale=2; $DF_TIME / $RDD_TIME" | bc)
    echo "RDD is ${SPEEDUP}x faster than DataFrame"
  fi
fi

echo "Testing completed successfully!"