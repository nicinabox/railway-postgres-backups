#!/bin/bash
# Wrapper to ensure config is loaded before running entrypoint
# Railway was bypassing entrypoint with startCommand

source /app/lib/logging.sh
source /app/lib/config.sh
source /app/lib/utils.sh

# Load config to export PG* variables
if ! load_config; then
    echo "ERROR: Configuration load failed"
    exit 1
fi

# Now exec the entrypoint with loaded env
exec /app/entrypoint.sh "$@"
