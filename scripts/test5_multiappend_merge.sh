#!/usr/bin/env bash
# Test 5: Concurrent appends + merge
# Usage: ./test5_multiappend_merge.sh INITIATOR_VM_NUM HYDFSFILE VM_NUM1 BUSINESS_NUM1 [VM_NUM2 BUSINESS_NUM2 ...]
# Example:
# ./test5_multiappend_merge.sh 1 demo_foo.txt 1 5 2 10 3 15 4 20

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOSTS_FILE="$SCRIPT_DIR/../hosts.txt"

INITIATOR_VM_NUM=${1:-}
HYDFSFILE=${2:-}
shift 2 || true
PAIRS=($@)
CLIENT="./client"

if [[ -z "$INITIATOR_VM_NUM" || -z "$HYDFSFILE" || ${#PAIRS[@]} -lt 2 || $((${#PAIRS[@]} % 2)) -ne 0 ]]; then
  echo "Usage: $0 INITIATOR_VM_NUM HYDFSFILE VM_NUM1 BUSINESS_NUM1 [VM_NUM2 BUSINESS_NUM2 ...]"
  echo "Example: $0 1 demo_foo.txt 1 5 2 10 3 15 4 20"
  echo "  This example uses 4 VMs appending 4 different business files:"
  echo "    - VM 1 appends business_5.txt"
  echo "    - VM 2 appends business_10.txt"
  echo "    - VM 3 appends business_15.txt"
  echo "    - VM 4 appends business_20.txt"
  echo ""
  echo "Parameters:"
  echo "  INITIATOR_VM_NUM: VM number (1-10) to initiate multiappend from"
  echo "  HYDFSFILE: HyDFS filename"
  echo "  Pairs of: VM_NUM (1-10) BUSINESS_NUM (file number)"
  echo ""
  echo "Your command: $0 $*"
  echo "Pairs detected: ${#PAIRS[@]} arguments (${#PAIRS[@]}/2 = $((${#PAIRS[@]}/2)) pairs)"
  exit 2
fi

# Read hosts from hosts.txt
if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "Error: $HOSTS_FILE not found"
  exit 1
fi

mapfile -t HOSTS < "$HOSTS_FILE"

# Validate initiator VM number
if [[ "$INITIATOR_VM_NUM" -lt 1 || "$INITIATOR_VM_NUM" -gt "${#HOSTS[@]}" ]]; then
  echo "Error: INITIATOR_VM_NUM must be between 1 and ${#HOSTS[@]}"
  exit 1
fi

INITIATOR="${HOSTS[$((INITIATOR_VM_NUM - 1))]}"

# Compose multiappend args: hydfsfile vm1 local1 vm2 local2 ...
# Convert VM numbers to hostnames and business numbers to file paths
MULTIARGS=("$HYDFSFILE")
NUM_PAIRS=$((${#PAIRS[@]}/2))

for ((i=0; i<$NUM_PAIRS; i++)); do
  vm_num_idx=$((i*2))
  business_num_idx=$((i*2 + 1))
  
  vm_num=${PAIRS[$vm_num_idx]}
  business_num=${PAIRS[$business_num_idx]}
  
  # Validate VM number
  if [[ "$vm_num" -lt 1 || "$vm_num" -gt "${#HOSTS[@]}" ]]; then
    echo "Error: VM_NUM $vm_num must be between 1 and ${#HOSTS[@]}"
    exit 1
  fi
  
  vm_host="${HOSTS[$((vm_num - 1))]}"
  business_file="/home/saik2/mp3-g02/business/business_${business_num}.txt"
  
  # Validate business file exists locally (checking if it's in the repo)
  if [[ ! -f "$SCRIPT_DIR/../business/business_${business_num}.txt" ]]; then
    echo "Error: Business file not found: business/business_${business_num}.txt"
    exit 1
  fi
  
  MULTIARGS+=("$vm_host" "$business_file")
done

# Step 1: Run multiappend from initiator (it will ssh out to each VM)
echo "Launching multiappend from VM $INITIATOR_VM_NUM ($INITIATOR)"
ssh -o LogLevel=ERROR "$INITIATOR" "cd /home/saik2/mp3-g02 && $CLIENT -cmd multiappend ${MULTIARGS[*]}"

# Step 2: Wait a little for appends to propagate
sleep 5

# Step 3: Run merge on initiator (or any node)
echo "\n== Running merge on VM $INITIATOR_VM_NUM ($INITIATOR) =="
ssh -o LogLevel=ERROR "$INITIATOR" "cd /home/saik2/mp3-g02 && $CLIENT -cmd merge '$HYDFSFILE'"

# Step 4: Get replica VMs from ls command
echo "\n== Getting replica information =="
LS_OUTPUT=$(ssh -o LogLevel=ERROR "$INITIATOR" "cd /home/saik2/mp3-g02 && $CLIENT -cmd ls '$HYDFSFILE'")
echo "$LS_OUTPUT"

# Extract the first two replica addresses from ls output
# Format: "  - 172.22.154.6:8082 (RingID: 2781537700)"
REPLICA_ADDRESSES=($(echo "$LS_OUTPUT" | grep -E '^\s+-\s+[0-9]+\.' | sed -E 's/^\s+-\s+([0-9.]+:[0-9]+).*/\1/' | head -2))

if [[ ${#REPLICA_ADDRESSES[@]} -lt 2 ]]; then
  echo "Error: Could not extract at least 2 replica addresses from ls output"
  exit 1
fi

VM_A="${REPLICA_ADDRESSES[0]}"
VM_B="${REPLICA_ADDRESSES[1]}"

OUT_A="/tmp/hy_replica_A"
OUT_B="/tmp/hy_replica_B"

echo "\n== Fetching from replica $VM_A and $VM_B =="
$CLIENT -cmd getfromreplica "$VM_A" "$HYDFSFILE" "$OUT_A"
$CLIENT -cmd getfromreplica "$VM_B" "$HYDFSFILE" "$OUT_B"

# Compare the two fetched files
echo "\n== Comparing the two replica fetches =="
diff -q "$OUT_A" "$OUT_B" && echo "Replicas are identical" || (echo "Replicas differ"; diff -u "$OUT_A" "$OUT_B" || true)

# Finally, check contents contain the append markers (grep for known keywords from local files)
echo "\n== Ensuring no appends lost (simple grep for sample words) =="
for ((i=0; i<$NUM_PAIRS; i++)); do
  vm_num_idx=$((i*2))
  business_num_idx=$((i*2 + 1))
  
  vm_num=${PAIRS[$vm_num_idx]}
  business_num=${PAIRS[$business_num_idx]}
  
  business_file="$SCRIPT_DIR/../business/business_${business_num}.txt"
  sample=$(head -n 1 "$business_file")
  echo "Looking for sample from business_${business_num}.txt: '$sample'"
  grep -nF "$sample" "$OUT_A" || true
done

echo "\n== Test 5 completed =="
exit 0
