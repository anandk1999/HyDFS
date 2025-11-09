#!/usr/bin/env bash
# Test 2: Get + correct replicas on the ring
# Usage: ./test2_get_and_replicas.sh HYDFS_FILENAME WRITER_VM READER_VM
# Example: ./test2_get_and_replicas.sh demo_business_1.txt vm1 vm2

set -euo pipefail

HYDFSFILE=${1:-}
WRITER_VM=${2:-localhost}
READER_VM=${3:-localhost}
CLIENT="./client"
LOCAL_DATA_DIR="/home/ubuntu/business"
OUTFILE="/tmp/hy_get_out"

if [[ -z "$HYDFSFILE" ]]; then
  echo "Usage: $0 HYDFSFILE WRITER_VM READER_VM"
  exit 2
fi

echo "== Test 2a: GET file from HyDFS (reader: $READER_VM) =="
# fetch from reader VM (client will write to OUTFILE on the reader)
if [[ "$READER_VM" == "localhost" || "$READER_VM" == "127.0.0.1" ]]; then
  cd /home/pnj2/mp3-g02 && $CLIENT -cmd get "$HYDFSFILE" "$OUTFILE"
else
  ssh "$READER_VM" "cd /home/pnj2/mp3-g02 && $CLIENT -cmd get '$HYDFSFILE' '$OUTFILE'"
fi

# Compare with local dataset copy on the reader VM
echo "== Diff against local dataset copy on $READER_VM =="
LOCAL_COPY_PATH="$LOCAL_DATA_DIR/${HYDFSFILE#demo_}"  # assumes hydfs filename corresponds to dataset file
if [[ "$READER_VM" == "localhost" || "$READER_VM" == "127.0.0.1" ]]; then
  diff -q "$LOCAL_COPY_PATH" "$OUTFILE" && echo "Files are identical" || (echo "Files differ"; exit 1)
else
  ssh "$READER_VM" "cd /home/pnj2/mp3-g02 && diff -q '$LOCAL_COPY_PATH' '$OUTFILE' && echo 'Files are identical' || (echo 'Files differ'; exit 1)"
fi

# Test 2b: show replicas on ring and membership (ls + list_mem_ids)
echo "\n== Ring & replica info (ls + list_mem_ids) on writer VM: $WRITER_VM =="
if [[ "$WRITER_VM" == "localhost" || "$WRITER_VM" == "127.0.0.1" ]]; then
  $CLIENT -cmd ls "$HYDFSFILE"
  echo "--- membership (sorted) ---"
  $CLIENT -cmd list_mem_ids
else
  ssh "$WRITER_VM" "cd $(pwd) && $CLIENT -cmd ls '$HYDFSFILE'"
  echo "--- membership (sorted) ---"
  ssh "$WRITER_VM" "cd $(pwd) && $CLIENT -cmd list_mem_ids"
fi

# Run liststore on a random replica VM (ask TA to pick one) or on the writer VM
echo "\n== liststore on chosen VM (using writer VM as default) =="
if [[ "$WRITER_VM" == "localhost" || "$WRITER_VM" == "127.0.0.1" ]]; then
  $CLIENT -cmd liststore
else
  ssh "$WRITER_VM" "cd $(pwd) && $CLIENT -cmd liststore"
fi

echo "\n== Test 2 completed =="
exit 0
