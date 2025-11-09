#!/usr/bin/env bash
# Test 3: Post-failure re-replication
# Usage: ./test3_rereplication.sh HYDFS_FILENAME REPLICA_VMS_TO_KILL... (at least 1)
# Example: ./test3_rereplication.sh demo_business_1.txt vm3 vm5

set -euo pipefail

HYDFSFILE=${1:-}
shift || true
KILL_VMS=("$@")
CLIENT="/home/pnj2/mp3-g02/client"
WAIT_AFTER_KILL=20   # seconds to wait for re-replication

if [[ -z "$HYDFSFILE" || ${#KILL_VMS[@]} -eq 0 ]]; then
  echo "Usage: $0 HYDFS_FILENAME VM_TO_KILL [VM2 ...]"
  exit 2
fi

# Helper to stop the service on a VM. We try a graceful stop (pkill), but this is demo-only.
stop_node() {
  local vm="$1"
  echo "Stopping HyDFS process on $vm"
  ssh "$vm" "pkill -f mp3-g02 || true; pkill -f main || true; pkill -f client || true"
}

# Step 1: Kill the given VMs
for v in "${KILL_VMS[@]}"; do
  stop_node "$v"
done

echo "Killed ${#KILL_VMS[@]} VM(s). Waiting $WAIT_AFTER_KILL seconds for the cluster to stabilize and re-replicate..."
sleep $WAIT_AFTER_KILL

# Step 2: On a surviving node, show ls, list_mem_ids, and liststore for the file
SURVIVING_VM=localhost
# pick first surviving host (user can edit this variable). If you want to run on a specific machine, edit SURVIVING_VM.

echo "\n== After failures: list_mem_ids on $SURVIVING_VM =="
if [[ "$SURVIVING_VM" == "localhost" ]]; then
  cd /home/pnj2/mp3-g02 && $CLIENT -cmd list_mem_ids
else
  ssh "$SURVIVING_VM" "cd /home/pnj2/mp3-g02 && $CLIENT -cmd list_mem_ids"
fi

echo "\n== ls (replicas) for $HYDFSFILE on $SURVIVING_VM =="
if [[ "$SURVIVING_VM" == "localhost" ]]; then
  $CLIENT -cmd ls "$HYDFSFILE"
else
  ssh "$SURVIVING_VM" "cd $(pwd) && $CLIENT -cmd ls '$HYDFSFILE'"
fi

echo "\n== liststore on $SURVIVING_VM =="
if [[ "$SURVIVING_VM" == "localhost" ]]; then
  $CLIENT -cmd liststore
else
  ssh "$SURVIVING_VM" "cd $(pwd) && $CLIENT -cmd liststore"
fi

echo "\n== Test 3 completed =="
exit 0
