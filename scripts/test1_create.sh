#!/usr/bin/env bash
# Test 1: Create 5 files sequentially
# Usage: ./test1_create.sh [WRITER_VM]
# If WRITER_VM is empty, runs locally.

set -euo pipefail

# === Config ===
WRITER_VM=${1:-"localhost"}   # hostname[:port] or localhost
CLIENT="./client"  # full path to the client binary
DATA_DIR="/home/saik2/mp3-g02/business" # path on each VM where business_[1-20] are located
FILES=("business_10.txt" "business_2.txt" "business_3.txt" "business_4.txt" "business_5.txt")
HYDFS_PREFIX="demo_"           # prefix to use for HyDFS filenames
SLEEP_AFTER_CREATE=3

# === Helpers ===
run_on() {
  local host="$1"; shift
  if [[ "$host" == "localhost" || "$host" == "127.0.0.1" ]]; then
    ./client
    (cd "$(pwd)" && "$@")
  else
    ssh "$host" "cd /home/saik2/mp3-g02 && $*"
  fi
}

echo "== Test 1: CREATE 5 files sequentially (writer: $WRITER_VM) =="

for f in "${FILES[@]}"; do
  localpath="$DATA_DIR/$f"
  hydfs="${HYDFS_PREFIX}${f}"
  echo "-> Creating $hydfs from $localpath on $WRITER_VM"
  run_on "$WRITER_VM" "$CLIENT -cmd create $localpath $hydfs"
  echo "   create issued for $hydfs"
  sleep $SLEEP_AFTER_CREATE
done

echo "== Test 1 completed: created ${#FILES[@]} files =="

exit 0
