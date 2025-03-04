#!/bin/bash

command_file="command_list.txt"
declare -A command_status
count=0

while IFS= read -r cmd; do
    # Explicitly run command in background
    (eval "$cmd") &
    pid=$!
    
    command_status["$pid"]="$cmd"
    
    ((count++))
    
    if ((count % 20 == 0)); then
        sleep 40
    fi
done < "$command_file"

# Wait for all background processes to finish
wait

success_count=0
failure_count=0

echo "Command Execution Results:"
for pid in "${!command_status[@]}"; do
    cmd="${command_status[$pid]}"
    if wait "$pid"; then
        echo "SUCCESS: $cmd"
        ((success_count++))
    else
        echo "FAILURE: $cmd"
        ((failure_count++))
    fi
done

echo -e "\nExecution Summary:"
echo "Total commands: $count"
echo "Successful: $success_count"
echo "Failed: $failure_count"
