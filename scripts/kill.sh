#!/bin/bash

# Kills anything listening on port 8080 across all hosts (plus common dev tools).
# Load remote username
SCRIPT_PATH=${0:A}
SCRIPT_DIR="$(cd "${SCRIPT_PATH:h}" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"
HOSTS_FILE="../hosts.txt"
PORT="8080"

# Read hosts
HOSTS=($(cat "$HOSTS_FILE"))

echo "Select normal nodes (space-separated numbers, or ENTER for all):"
OTHER_HOSTS=()
for ((i = 1; i <= ${#HOSTS[@]}; i++)); do
    host=${HOSTS[$i]}
    OTHER_HOSTS+=("$host")
    echo "  ${#OTHER_HOSTS[@]}. $host"
done

selected="$*"
if [[ -z "$selected" ]]; then
    printf "Enter numbers: "
    read selected
fi
if [ -z "$selected" ]; then
    NORMAL_NODES=("${OTHER_HOSTS[@]}")
else
    NORMAL_NODES=()
    for num in $selected; do
        if [[ "$num" =~ ^[0-9]+$ ]] && (( num >= 1 && num <= ${#OTHER_HOSTS[@]} )); then
            NORMAL_NODES+=("${OTHER_HOSTS[$num]}")
        else
            echo "Skipping invalid selection: $num"
        fi
    done
fi

# Start normal nodes in parallel
echo "Starting ${#NORMAL_NODES[@]} normal nodes..."
for host in "${NORMAL_NODES[@]}"; do
    echo ">>> Killing process on port 8080 at $host"
    ssh "$REMOTE_USER@$host" "pkill -f client" &
done

wait
echo "Killed processes in selected nodes!"