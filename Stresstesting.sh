#!/bin/bash

# Variables
LOG_FILE="script_execution.log"
EXECUTION_TIME=900  # 15 minutes in seconds
TOTAL_EXECUTIONS=24  # 6 hours / 15 minutes

# Function to log script execution
log_execution() {
  local script_name="$1"
  local status="$2"

  echo "$(date +"%Y-%m-%d %H:%M:%S") - $script_name: $status" >> "$LOG_FILE"
}

# Function to execute scripts in background
execute_scripts() {
  local i
  local success_count=0
  local failure_count=0

  for ((i=1; i<=10; i++)); do
    ./script_$i.sh &  # Replace with your script names
    log_execution "script_$i.sh" "started"
  done

  wait  # Wait for all background jobs to finish

  for job in $(jobs -p); do
    if kill -0 $job 2> /dev/null; then
      ((success_count++))
      log_execution "Job $job" "succeeded"
    else
      ((failure_count++))
      log_execution "Job $job" "failed"
    fi
  done

  echo "$(date +"%Y-%m-%d %H:%M:%S") - Total successful jobs: $success_count" >> "$LOG_FILE"
  echo "$(date +"%Y-%m-%d %H:%M:%S") - Total failed jobs: $failure_count" >> "$LOG_FILE"
}

# Main loop
for ((i=1; i<=TOTAL_EXECUTIONS; i++)); do
  execute_scripts
  sleep $EXECUTION_TIME
done
