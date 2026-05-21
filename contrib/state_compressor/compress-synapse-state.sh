#!/bin/bash
# Wrapper script to run rust-synapse-compress-state
# Download the binary from: https://github.com/matrix-org/rust-synapse-compress-state/releases

set -e

# Configuration
SYNAPSE_DB_URI="postgresql://synapse:password@/var/run/postgresql/synapse"
# Or if using PgBouncer TCP:
# SYNAPSE_DB_URI="postgresql://synapse:password@localhost:6432/synapse"

# Path to the downloaded binary
COMPRESS_TOOL="/usr/local/bin/rust-synapse-compress-state"

if [ ! -x "$COMPRESS_TOOL" ]; then
    echo "Error: $COMPRESS_TOOL not found or not executable. Please download it first."
    exit 1
fi

echo "Starting state compression at $(date)"

# Run the compressor. The arguments depend on your specific needs and DB size.
# Example: -p is for postgres. -r compresses room by room.
"$COMPRESS_TOOL" -p "$SYNAPSE_DB_URI" -c -t -o

echo "State compression finished at $(date)"
