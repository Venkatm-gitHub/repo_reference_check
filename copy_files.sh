#!/bin/bash

# Define source and target directories
SOURCE_INCREMENTAL="/var/incremental"
SOURCE_FULL="/var/full"
TARGET_BASE="/var/merged/stage"
TARGET_INCREMENTAL="$TARGET_BASE/incremental"
TARGET_FULL="$TARGET_BASE/full"

# Create the target directories
mkdir -p "$TARGET_INCREMENTAL"
mkdir -p "$TARGET_FULL"

# Copy files from source to target directories
cp -r "$SOURCE_INCREMENTAL/"* "$TARGET_INCREMENTAL/"
cp -r "$SOURCE_FULL/"* "$TARGET_FULL/"

# Change permissions to 775 for the copied files
chmod -R 775 "$TARGET_INCREMENTAL"
chmod -R 775 "$TARGET_FULL"

# Execute the Python script with the target directories as arguments
python3 your_script.py "$TARGET_INCREMENTAL" "$TARGET_FULL"

# Check if the Python script executed successfully
if [ $? -eq 0 ]; then
    # Delete staged files if the script was successful
    rm -rf "$TARGET_INCREMENTAL/"*
    rm -rf "$TARGET_FULL/"*
else
    echo "Python script execution failed. Staged files not deleted."
fi
