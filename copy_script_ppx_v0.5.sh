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
  mkdir -p "$tgt_incremental" "$tgt_full"
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
  local retry_delay=300

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

# Function to validate md5sum of copied files
validate_md5sum() {
  local src="$1"
  local dst="$2"

  src_md5=$(md5sum "$src" | awk '{ print $1 }')
  dst_md5=$(md5sum "$dst" | awk '{ print $1 }')

  if [[ "$src_md5" == "$dst_md5" ]]; then
    log "MD5 sum matched for $src and $dst."
    return 0
  else
    log "MD5 sum mismatch for $src and $dst."
    return 1
  fi
}

# Function to extract target tables from dlemerge.json based on source table name
get_target_tables() {
  local source_table="$1"

  # Extract lines containing the source table and check for Foreign_keys section.
  grep -A10 "\"source_table\": \"$source_table\"" dlemerge.json | \
    awk '/"Foreign_keys"/ {found=1} found && /\[/ {getline; while (!/\]/) {if (/target_table/) {gsub(/.*"target_table": *"/, "", $0); gsub(/".*/, "", $0); printf "%s ", $0;} getline;} found=0;}' | \
    tr '\n' ' '
}

# Main processing function based on argument type (INTRA or NONINTRA)
process_argument() {
  local arg="$1"
  
  # Read values from appl.conf based on the argument type
  if [[ "$arg" == "INTRA" ]]; then
    values=$(grep 'INTRA' appl.conf | cut -d'=' -f2 | tr -d '"' | tr ',' ' ')
    
  elif [[ "$arg" == "NONINTRA" ]]; then
    values=$(grep 'NONINTRA' appl.conf | cut -d'=' -f2 | tr -d '"' | tr ',' ' ')
    
  else
    log "Invalid argument: $arg. Exiting."
    exit 1
  fi

  # Process each value obtained from appl.conf
  for value in $values; do
    match_found=false
    
    # Search in dlemerge.json for the source_table matching the value
    source_table=$(grep -o "\"source_table\": \"$value\"" dlemerge.json)
    
    if [[ -n "$source_table" ]]; then
      match_found=true
      
      # Get corresponding target tables from dlemerge.json using the helper function
      target_tables=$(get_target_tables "$value")
      
      # Copy files based on source and target tables found
      src_file="${src_incremental}/${value}.hyper"
      tgt_file="${tgt_incremental}/${value}.hyper"
      copy_file "$src_file" "$tgt_file"

      # Validate md5sum after copying incremental files
      validate_md5sum "$src_file" "$tgt_file"

      for target in $target_tables; do 
        src_full_file="${src_full}/${target}.hyper"
        tgt_full_file="${tgt_full}/${target}.hyper"
        copy_file "$src_full_file" "$tgt_full_file"

        # Validate md5sum after copying full files 
        validate_md5sum "$src_full_file" "$tgt_full_file"
      done
      
    fi
    
    if ! $match_found; then
      log "Error: Value '$value' obtained from conf is not available in dlemerge.json."
      exit 1
    fi
    
  done
  
}

# Main function to handle script execution flow
main() {
  
  # Check if exactly one argument is passed 
  if [ $# -ne 1 ]; then 
    log "Error: Exactly one argument is required."
    exit 1 
  fi
  
  create_target_dir
  
  process_argument "$1"
  
}

main "$@"