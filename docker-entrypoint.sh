#!/bin/bash
set -e
# Extract the number and add 1 to get Raft node IDs 1, 2, 3...

# The StatefulSet hostname will be like "raft-0", "raft-1", etc.
ORDINAL=${HOSTNAME##*-}
NODE_ID=$((ORDINAL + 1))

PORT=8000

# The PEERS environment variable should be passed in via ConfigMap
# Example: 1=raft-0.raft-svc.raft.svc.cluster.local:8000,2=...
if [ -z "$PEERS" ]; then
    echo "PEERS environment variable not set! Please mount it via ConfigMap."
    exit 1
fi

echo "Starting Raft Node $NODE_ID on port $PORT"
echo "Peers: $PEERS"

exec server --id $NODE_ID --port $PORT --peers $PEERS
