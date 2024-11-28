#!/bin/bash

# Set source and destination paths
SOURCE_FILE="/path/to/backups/restapi_org.py"
DESTINATION_DIR="/path/to/destination/directory"
DESTINATION_FILE="${DESTINATION_DIR}/restapi_org.py"

# Function to exit with error message
exit_with_error() {
    echo "ERROR: $1" >&2
    exit 1
}

# Validate source file exists
if [ ! -f "$SOURCE_FILE" ]; then
    exit_with_error "Source backup file does not exist: $SOURCE_FILE"
fi

# Check if source file is readable
if [ ! -r "$SOURCE_FILE" ]; then
    exit_with_error "Source backup file is not readable: $SOURCE_FILE"
fi

# Validate destination directory exists
if [ ! -d "$DESTINATION_DIR" ]; then
    exit_with_error "Destination directory does not exist: $DESTINATION_DIR"
fi

# Check write permissions on destination directory
if [ ! -w "$DESTINATION_DIR" ]; then
    exit_with_error "No write permission on destination directory: $DESTINATION_DIR"
fi

# Check if destination file already exists
if [ -f "$DESTINATION_FILE" ]; then
    # Optional: Ask for confirmation before overwriting
    read -p "Destination file already exists. Overwrite? (y/n): " confirm
    if [[ $confirm != [yY] && $confirm != [yY][eE][sS] ]]; then
        exit_with_error "File copy cancelled by user"
    fi
fi

# Check source file size
SOURCE_SIZE=$(stat -c%s "$SOURCE_FILE")
MIN_FILE_SIZE=10  # Minimum file size in bytes
MAX_FILE_SIZE=$((10 * 1024 * 1024))  # Maximum 10MB

if [ "$SOURCE_SIZE" -lt "$MIN_FILE_SIZE" ]; then
    exit_with_error "Source file is too small (less than $MIN_FILE_SIZE bytes)"
fi

if [ "$SOURCE_SIZE" -gt "$MAX_FILE_SIZE" ]; then
    exit_with_error "Source file is too large (greater than $MAX_FILE_SIZE bytes)"
fi

# Perform the file copy
cp "$SOURCE_FILE" "$DESTINATION_FILE"

# Verify the copy was successful
if [ $? -ne 0 ]; then
    exit_with_error "Failed to copy the file"
fi

# Additional verification of copied file
if [ ! -f "$DESTINATION_FILE" ]; then
    exit_with_error "Copy verification failed: Destination file not found"
fi

# Compare file contents to ensure integrity
if ! cmp -s "$SOURCE_FILE" "$DESTINATION_FILE"; then
    exit_with_error "File copy integrity check failed"
fi

# Success message
echo "Backup file successfully copied to $DESTINATION_FILE"

exit 0
