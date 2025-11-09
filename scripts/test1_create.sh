#!/usr/bin/env bash
# Test 1: Create 5 files sequentially
# Usage: ./test1_create.sh WRITER_VM_NUM BUSINESS_NUM1 BUSINESS_NUM2 BUSINESS_NUM3 BUSINESS_NUM4 BUSINESS_NUM5
# Example: ./test1_create.sh 1 10 2 3 4 5

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOSTS_FILE="$SCRIPT_DIR/../hosts.txt"

# === Config ===
WRITER_VM_NUM=${1:-}
BUSINESS_NUM1=${2:-}
BUSINESS_NUM2=${3:-}
BUSINESS_NUM3=${4:-}
BUSINESS_NUM4=${5:-}
BUSINESS_NUM5=${6:-}

if [[ -z "$WRITER_VM_NUM" || -z "$BUSINESS_NUM1" || -z "$BUSINESS_NUM2" || -z "$BUSINESS_NUM3" || -z "$BUSINESS_NUM4" || -z "$BUSINESS_NUM5" ]]; then
  echo "Usage: $0 WRITER_VM_NUM BUSINESS_NUM1 BUSINESS_NUM2 BUSINESS_NUM3 BUSINESS_NUM4 BUSINESS_NUM5"
  echo "Example: $0 1 10 2 3 4 5"
  echo "  WRITER_VM_NUM: VM number (1-10) to create files from"
  echo "  BUSINESS_NUM1-5: Business file numbers (e.g., 10 for business_10.txt)"
  exit 2
fi

# Read hosts from hosts.txt
if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "Error: $HOSTS_FILE not found"
  exit 1
fi

mapfile -t HOSTS < "$HOSTS_FILE"

# Validate VM number
if [[ "$WRITER_VM_NUM" -lt 1 || "$WRITER_VM_NUM" -gt "${#HOSTS[@]}" ]]; then
  echo "Error: WRITER_VM_NUM must be between 1 and ${#HOSTS[@]}"
  exit 1
fi

WRITER_VM="${HOSTS[$((WRITER_VM_NUM - 1))]}"

CLIENT="./client"
DATA_DIR="/home/saik2/mp3-g02/business"
BUSINESS_NUMS=("$BUSINESS_NUM1" "$BUSINESS_NUM2" "$BUSINESS_NUM3" "$BUSINESS_NUM4" "$BUSINESS_NUM5")
HYDFS_PREFIX="demo_"
SLEEP_AFTER_CREATE=3

# Validate all business files exist locally
for num in "${BUSINESS_NUMS[@]}"; do
  if [[ ! -f "$SCRIPT_DIR/../business/business_${num}.txt" ]]; then
    echo "Error: Business file not found: business/business_${num}.txt"
    exit 1
  fi
done

# === Helpers ===
run_on() {
  local host="$1"; shift
  ssh -o LogLevel=ERROR "$host" "cd /home/saik2/mp3-g02 && $*"
}

echo "== Test 1: CREATE 5 files sequentially (writer: VM $WRITER_VM_NUM - $WRITER_VM) =="

for num in "${BUSINESS_NUMS[@]}"; do
  filename="business_${num}.txt"
  localpath="$DATA_DIR/$filename"
  hydfs="${HYDFS_PREFIX}${filename}"
  echo "-> Creating $hydfs from $localpath on $WRITER_VM"
  run_on "$WRITER_VM" "$CLIENT -cmd create $localpath $hydfs"
  echo "   create issued for $hydfs"
  sleep $SLEEP_AFTER_CREATE
done

echo "== Test 1 completed: created ${#BUSINESS_NUMS[@]} files =="

exit 0
