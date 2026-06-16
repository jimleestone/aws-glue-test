#!/bin/bash

# Define your standalone parameters
QUEUE_NAME="test-sqs-monitor"
MAX_EXECUTE_COUNT=3
INTERVAL=5
MAX_WAIT=20

# Define your array
JOB_LIST=("test-sqs" "another-test-sqs")

# Execute the script, passing the array LAST
./test_loop.sh "$QUEUE_NAME" "$MAX_EXECUTE_COUNT" "$INTERVAL" "$MAX_WAIT" "${JOB_LIST[@]}" >output.log 2>&1
