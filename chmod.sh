#!/bin/bash

# Function to safely create directory with proper permissions
setup_log_directory() {
    local env_name="$1"
    local base_dir="/var/tmp"
    local log_dir="$base_dir/logs"
    local env_dir="$log_dir/$env_name"

    # Check if environment name is provided
    if [ -z "$env_name" ]; then
        echo "Error: Environment name not provided"
        exit 1
    }

    # Create logs directory if it doesn't exist
    if [ ! -d "$log_dir" ]; then
        mkdir "$log_dir" 2>/dev/null
        if [ $? -eq 0 ]; then
            chmod 775 "$log_dir"
            echo "Created $log_dir with 775 permissions"
        fi
    fi

    # Verify logs directory exists and is writable
    if [ ! -d "$log_dir" ]; then
        echo "Error: Failed to create or access $log_dir"
        exit 1
    fi

    # Try to chmod logs directory, but ignore errors
    chmod 775 "$log_dir" 2>/dev/null

    # Create environment-specific directory
    if [ ! -d "$env_dir" ]; then
        mkdir "$env_dir"
        if [ $? -eq 0 ]; then
            chmod 775 "$env_dir"
            echo "Created $env_dir with 775 permissions"
        else
            echo "Error: Failed to create $env_dir"
            exit 1
        fi
    else
        echo "Directory $env_dir already exists"
    fi
}

# Get environment name from command line argument
ENV_NAME="$1"

# Call the setup function
setup_log_directory "$ENV_NAME"
