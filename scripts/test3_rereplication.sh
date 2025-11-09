#!/usr/bin/env bash
# Test 3: Post-failure re-replication
# Usage: ./test3_rereplication.sh HYDFS_FILENAME VM_NUMBER_TO_KILL... (at least 1)
# Example: ./test3_rereplication.sh demo_business_1.txt 3 5
# VM numbers are 1-10 corresponding to lines in hosts.txt

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOSTS_FILE="${SCRIPT_DIR}/../hosts.txt"

HYDFSFILE=${1:-}
shift || true
KILL_VM_NUMS=("$@")
CLIENT="./client"
WAIT_AFTER_KILL=20   # seconds to wait for re-replication

if [[ -z "$HYDFSFILE" || ${#KILL_VM_NUMS[@]} -eq 0 ]]; then
  echo "Usage: $0 HYDFS_FILENAME VM_NUMBER_TO_KILL [VM_NUMBER2 ...]"
  echo "VM numbers are 1-10 (corresponding to lines in hosts.txt)"
  exit 2
fi

# Ensure hosts file exists
if [[ ! -f "$HOSTS_FILE" ]]; then
    echo "Error: hosts.txt not found at $HOSTS_FILE"
    exit 1
fi

# Read hosts into array
HOSTS=()
while IFS= read -r line; do
    HOSTS+=("$line")
done < "$HOSTS_FILE"

# Helper to stop the service on a VM. We try a graceful stop (pkill), but this is demo-only.
stop_node() {
  local vm="$1"
  echo "Stopping HyDFS process on $vm"
  ssh "$vm" "pkill -f mp3-g02 || true; pkill -f main || true; pkill -f client || true"
}

# Step 1: Kill the given VMs (convert VM numbers to hostnames)
for vm_num in "${KILL_VM_NUMS[@]}"; do
  if [[ "$vm_num" -lt 1 || "$vm_num" -gt ${#HOSTS[@]} ]]; then
    echo "Error: Invalid VM number $vm_num. Must be between 1 and ${#HOSTS[@]}"
    exit 1
  fi
  
  # Bash arrays are 0-indexed, so subtract 1
  vm_host="${HOSTS[$((vm_num - 1))]}"
  echo "VM $vm_num -> $vm_host"
  stop_node "$vm_host"
done

echo "Killed ${#KILL_VM_NUMS[@]} VM(s). Waiting $WAIT_AFTER_KILL seconds for the cluster to stabilize and re-replicate..."
sleep $WAIT_AFTER_KILL

# Step 2: On a surviving node, show ls, list_mem_ids, and liststore for the file
# Use VM 1 as the surviving node (assuming it wasn't killed)
SURVIVING_VM="${HOSTS[0]}"
echo "\n== Using VM 1 ($SURVIVING_VM) as surviving node for queries =="

echo "\n== After failures: list_mem_ids on $SURVIVING_VM =="
ssh "$SURVIVING_VM" "cd /home/saik2/mp3-g02 && $CLIENT -cmd list_mem_ids"

echo "\n== ls (replicas) for $HYDFSFILE on $SURVIVING_VM =="
ssh "$SURVIVING_VM" "cd /home/saik2/mp3-g02 && $CLIENT -cmd ls '$HYDFSFILE'"

echo "\n== liststore on $SURVIVING_VM =="
ssh "$SURVIVING_VM" "cd /home/saik2/mp3-g02 && $CLIENT -cmd liststore"

echo "\n== Test 3 completed =="
exit 0
