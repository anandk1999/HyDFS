#!/usr/bin/env bash
# Test 5: Concurrent appends + merge
# Usage: ./test5_multiappend_merge.sh INITIATOR_VM HYDFSFILE VM1 LOCAL1 VM2 LOCAL2 VM3 LOCAL3 VM4 LOCAL4
# Example:
# ./test5_multiappend_merge.sh vm1 demo_foo.txt vm1 /home/ubuntu/local/a.txt vm2 /home/ubuntu/local/b.txt vm3 /home/ubuntu/local/c.txt vm4 /home/ubuntu/local/d.txt

set -euo pipefail

INITIATOR=${1:-localhost}
HYDFSFILE=${2:-}
shift 2 || true
PAIRS=($@)
CLIENT="./client"

if [[ -z "$HYDFSFILE" || ${#PAIRS[@]} -lt 2 || $((${#PAIRS[@]} % 2)) -ne 0 ]]; then
  echo "Usage: $0 INITIATOR_VM HYDFSFILE VM1 LOCAL1 [VM2 LOCAL2 ...]"
  exit 2
fi

# Compose multiappend args: hydfsfile vm1 local1 vm2 local2 ...
MULTIARGS=("$HYDFSFILE")
for p in "${PAIRS[@]}"; do
  MULTIARGS+=("$p")
done

# Step 1: Run multiappend from initiator (it will ssh out to each VM)
if [[ "$INITIATOR" == "localhost" || "$INITIATOR" == "127.0.0.1" ]]; then
  echo "Launching multiappend from local initiator"
  $CLIENT -cmd multiappend "${MULTIARGS[@]}"
else
  echo "Launching multiappend from $INITIATOR"
  ssh -o LogLevel=ERROR "$INITIATOR" "cd /home/saik2/mp3-g02 && $CLIENT -cmd multiappend ${MULTIARGS[*]}"
fi

# Step 2: Wait a little for appends to propagate
sleep 5

# Step 3: Run merge on initiator (or any node)
echo "\n== Running merge on $INITIATOR =="
if [[ "$INITIATOR" == "localhost" || "$INITIATOR" == "127.0.0.1" ]]; then
  $CLIENT -cmd merge "$HYDFSFILE"
else
  ssh -o LogLevel=ERROR "$INITIATOR" "cd /home/saik2/mp3-g02 && $CLIENT -cmd merge '$HYDFSFILE'"
fi

# Step 4: Fetch file from two replicas (ask TA to pick two replica VMs) and compare
# For convenience we will fetch from two VMs supplied as the last pair args (if available)
NUM_PAIRS=$((${#PAIRS[@]}/2))
if [[ $NUM_PAIRS -lt 2 ]]; then
  echo "Warning: less than two appenders specified; still attempting getfromreplica from first two VMs if present"
fi

VM_A=${PAIRS[0]}
VM_B=${PAIRS[2]:-${PAIRS[0]}}
OUT_A="/tmp/hy_replica_A"
OUT_B="/tmp/hy_replica_B"

echo "\n== Fetching from replica VM A: $VM_A and VM B: $VM_B =="
if [[ "$VM_A" == "localhost" || "$VM_A" == "127.0.0.1" ]]; then
  $CLIENT -cmd getfromreplica "$VM_A" "$HYDFSFILE" "$OUT_A"
else
  $CLIENT -cmd getfromreplica "$VM_A" "$HYDFSFILE" "$OUT_A"
fi

if [[ "$VM_B" == "localhost" || "$VM_B" == "127.0.0.1" ]]; then
  $CLIENT -cmd getfromreplica "$VM_B" "$HYDFSFILE" "$OUT_B"
else
  $CLIENT -cmd getfromreplica "$VM_B" "$HYDFSFILE" "$OUT_B"
fi

# Compare the two fetched files
echo "\n== Comparing the two replica fetches =="
diff -q "$OUT_A" "$OUT_B" && echo "Replicas are identical" || (echo "Replicas differ"; diff -u "$OUT_A" "$OUT_B" || true)

# Finally, check contents contain the append markers (grep for known keywords from local files)
echo "\n== Ensuring no appends lost (simple grep for sample words) =="
for ((i=1;i<=$NUM_PAIRS;i++)); do
  idx=$(( (i-1)*2 + 1 ))
  vm=${PAIRS[$((idx-1))]}
  localfile=${PAIRS[$idx]}
  sample=$(ssh -o LogLevel=ERROR ${vm%%:*} "head -n 1 '$localfile'" 2>/dev/null || head -n 1 "$localfile")
  echo "Looking for sample from $localfile: '$sample'"
  grep -nF "$sample" "$OUT_A" || true
done

echo "\n== Test 5 completed =="
exit 0
