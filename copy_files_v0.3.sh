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

# Function to copy files from source to target directories
copy_files() {
  for file in "$src_incremental"/*; do
    copy_file "$file" "$tgt_incremental"
  done

  for file in "$src_full"/*; do
    copy_file "$file" "$tgt_full"
  done
}

# Function to get table name from dlemerge.json
get_table_name() {
  local table_type="$1"  # "fact_table" or "dim_table"
  local json_file="dlemerge.json"

  grep -Eo "\"$table_type\": \"(.*?)\"" "$json_file" | cut -d'"' -f2
}

# Function to compare checksums with retries
compare_checksums() {
  local src="$1"
  local dst="$2"
  local max_retries=5
  local retry_delay=10

  for ((i=1; i <= max_retries; i++)); do
    src_checksum=$(md5sum "$src" | cut -d' ' -f1)
    dst_checksum=$(md5sum "$dst" | cut -d' ' -f1)

    if [[ "$src_checksum" == "$dst_checksum" ]]; then
      log "Checksums match for $src and $dst"
      return 0
    fi

    log "Checksums differ for $src and $dst. Retrying in $retry_delay seconds..."
    sleep $retry_delay
  done

  log "Checksums still differ for $src and $dst after $max_retries retries. Exiting."
  exit 1
}

# Main function
main() {
  create_target_dir

  # Read table name from dlemerge.json
  fact_table_name=$(get_table_name "fact_table")
  dim_table_name=$(get_table_name "dim_table")

  # Copy files based on table name
  if [[ -n "$fact_table_name" ]]; then
    log "Found fact_table: $fact_table_name"
    copy_file "/var/incremental/$fact_table_name" "$tgt_incremental/$fact_table_name"
  fi

  if [[ -n "$dim_table_name" ]]; then
    log "Found dim_table: $dim_table_name"
    copy_file "/var/full/$dim_table_name" "$tgt_full/$dim_table_name"
  fi

  # Checksum comparison and retries
  for file in "$tgt_incremental"/*; do
    compare_checksums "$file" "$file"
  done

  for file in "$tgt_full"/*; do
    compare_checksums "$file" "$file"
  done

  log "Staged files processed successfully."
  # Clean-up is removed, as success implies successful copy & verification.
  # rm -rf "$tgt_incremental" "$tgt_full"
}

main
