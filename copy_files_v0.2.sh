#!/bin/bash

# Define source and target directories
src_incremental="/var/incremental"
src_full="/var/full"
tgt_incremental="/var/merged/stage/incremental"
tgt_full="/var/merged/stage/full"
log_file="copy_script.log"

# Function to log a message
log() {
  echo "$(date +"%Y-%m-%d %H:%M:%S") $1" >> "$log_file"
}

# Function to create target directory structure
create_target_dir() {
  mkdir -p "$tgt_incremental"
  mkdir -p "$tgt_full"
  if [ $? -ne 0 ]; then
    log "Error creating target directories"
    exit 1
  fi
  chmod -R 775 /var/merged
}

# Function to copy a single file with retries
copy_file() {
  local src="$1"
  local dst="$2"
  local max_retries=5
  local retry_delay=10

  for ((i=1; i <= max_retries; i++)); do
    cp "$src" "$dst"
    if [ $? -eq 0 ]; then
      log "Copied $src to $dst"
      return 0
    fi

    log "Failed to copy $src. Retrying in $retry_delay seconds..."
    sleep $retry_delay
  done

  log "Failed to copy $src after $max_retries retries. Exiting."
  exit 1
}

# Function to copy files from source to target directories in parallel
copy_files() {
  for src_dir in "$src_incremental" "$src_full"; do
    tgt_dir="${tgt_incremental}"  # Set target dir based on source dir
    if [[ "$src_dir" == "$src_full" ]]; then
      tgt_dir="${tgt_full}"
    fi

    # Create a temporary file to track processed files
    temp_file="processed_files.txt"

    # Get a list of files, excluding subdirectories
    files=($(find "$src_dir" -maxdepth 1 -type f))

    # Spawn subprocesses to copy files in parallel
    for file in "${files[@]}"; do
      # Check if the file has already been processed
      if grep -q "$file" "$temp_file"; then
        continue
      fi

      tgt_file="${tgt_dir}/${file##*/}"
      (copy_file "$file" "$tgt_file") &
      echo "$file" >> "$temp_file"
    done

    # Wait for all subprocesses to finish
    wait

    # Remove the temporary file
    rm "$temp_file"
  done
}

# Function to execute Python script
execute_python_script() {
  python /path/to/your/python/script.py
  if [ $? -ne 0 ]; then
    log "Error executing Python script. Cleaning up staged files..."
    rm -rf "$tgt_incremental" "$tgt_full"
    exit 1
  fi
}

# Main function
main() {
  create_target_dir
  copy_files
  execute_python_script
  log "Staged files processed successfully."
  rm -rf "$tgt_incremental" "$tgt_full"
}

main
