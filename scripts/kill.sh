#!/bin/bash

# Kills anything listening on port 8080 across all hosts (plus common dev tools).
# Load remote username
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"
HOSTS_FILE="../hosts.txt"
PORT="8080"

# Read hosts
HOSTS=($(cat "$HOSTS_FILE"))

echo "Select normal nodes (space-separated numbers, or ENTER for all):"
OTHER_HOSTS=()
for i in "${!HOSTS[@]}"; do
    OTHER_HOSTS+=("${HOSTS[$i]}")
    echo "  ${#OTHER_HOSTS[@]}. ${HOSTS[$i]}"
done

read -p "Enter numbers: " selected
if [ -z "$selected" ]; then
    NORMAL_NODES=("${OTHER_HOSTS[@]}")
else
    NORMAL_NODES=()
    for num in $selected; do
        NORMAL_NODES+=("${OTHER_HOSTS[$((num - 1))]}")
    done
fi

# Start normal nodes in parallel
echo "Starting ${#NORMAL_NODES[@]} normal nodes..."
for host in "${NORMAL_NODES[@]}"; do
    echo ">>> Killing process on port 8080 at $HOST"
    ssh "$REMOTE_USER@$host" "pkill -f mp2-node" &
done

wait
echo "Killed processes in selected nodes!"