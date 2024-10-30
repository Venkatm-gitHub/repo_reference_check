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

# Function to check arguments and process based on INTRA or NONINTRA
process_arguments() {
  if [ "$#" -ne 1 ]; then
    echo "Error: Exactly one argument expected."
    exit 1
  fi

  source ./appl.conf

  if [ "$1" == "INTRA" ]; then
    values=$(echo $INTRA | tr ',' ' ')
    search_key="INTRA"
  elif [ "$1" == "NONINTRA" ]; then
    values=$(echo $NONINTRA | tr ',' ' ')
    search_key="NONINTRA"
  else
    echo "Error: First argument must be either INTRA or NONINTRA."
    exit 1
  fi

  process_values "$values" "$search_key"
}

# Function to process values based on the search key (INTRA/NONINTRA)
process_values() {
  local values="$1"
  local search_key="$2"

  for value in $values; do
    match=$(jq -r ".dle_merge_hyper[] | select(.source_table == \"$value\") | .foreign_keys[].target_table" dlemerge.json)

    if [ -z "$match" ]; then
      echo "Error: Value obtained from conf is not available in dlemerge.json for $value."
      exit 1
    else
      log "Match found for $value: $match"

      # Copy files based on the matches found in dlemerge.json
      copy_files_based_on_match "$value" "$match" "$search_key"
    fi
  done
}

# Function to copy files based on matches found in dlemerge.json
copy_files_based_on_match() {
  local source_value="$1"
  local target_tables="$2"
  local search_key="$3"

  # Prepare target table collection and source table collection variables.
  source_files=()
  target_files=()

  # Collect source and target files based on the match.
  for target_table in $target_tables; do
    source_files+=("${source_value}.hyper")
    target_files+=("${target_table}.hyper")
    
    # Copy logic based on INTRA or NONINTRA.
    if [ "$search_key" == "INTRA" ]; then
      cp "${src_incremental}/${source_value}.hyper" "${tgt_incremental}/${source_value}.hyper"
      cp "${src_full}/${target_table}.hyper" "${tgt_full}/${target_table}.hyper"
    else # NONINTRA case.
      cp "${src_incremental}/${source_value}.hyper" "${tgt_incremental}/${source_value}.hyper"
      cp "${src_full}/${target_table}.hyper" "${tgt_full}/${target_table}.hyper"
    fi  
  done

  # Call function to verify md5sum after copying.
  verify_md5sum "${source_files[@]}" "${target_files[@]}"
}

# Function to verify md5sum of copied files.
verify_md5sum() {
  local source_files=("$@")
  
  for file in "${source_files[@]}"; do
    md5src=$(md5sum "$src_incremental/${file##*/}" | awk '{ print $1 }')
    
    md5tgt=$(md5sum "${tgt_incremental}/${file##*/}" | awk '{ print $1 }')
    
    if [ "$md5src" == "$md5tgt" ]; then
      log "md5sum matched for ${file##*/}"
    else
      log "md5sum mismatched for ${file##*/}"
      
      for ((retry=1; retry<=5; retry++)); do 
        sleep 300 
        md5tgt_recheck=$(md5sum "${tgt_incremental}/${file##*/}" | awk '{ print $1 }')
        
        if [ "$md5src" == "$md5tgt_recheck" ]; then 
          log "md5sum matched after retry for ${file##*/}"
          break 
        fi 
      done
      
      if [ "$retry -eq 5" ]; then 
        log "Failed to match md5sum for ${file##*/} after retries."
        exit 1 
      fi 
    fi 
  done 
}

# Main function
main() {
  create_target_dir
  process_arguments "$@"
  
  log "Staged files processed successfully."
  
}

main "$@"