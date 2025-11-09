#!/usr/bin/env bash
# Test 4: Read-my-writes + client-append ordering
# Usage: ./test4_append_ordering.sh VM_NUM HYDFSFILE BUSINESS_NUM1 BUSINESS_NUM2
# Example: ./test4_append_ordering.sh 1 demo_foo.txt 5 10

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOSTS_FILE="$SCRIPT_DIR/../hosts.txt"

VM_NUM=${1:-}
HYDFSFILE=${2:-}
BUSINESS_NUM1=${3:-}
BUSINESS_NUM2=${4:-}
CLIENT="./client"
TMP_OUT="/tmp/hy_get_after_appends"

if [[ -z "$VM_NUM" || -z "$HYDFSFILE" || -z "$BUSINESS_NUM1" || -z "$BUSINESS_NUM2" ]]; then
  echo "Usage: $0 VM_NUM HYDFSFILE BUSINESS_NUM1 BUSINESS_NUM2"
  echo "Example: $0 1 demo_foo.txt 5 10"
  echo "  VM_NUM: VM number (1-10)"
  echo "  HYDFSFILE: HyDFS filename to create/append to"
  echo "  BUSINESS_NUM1: First business file number (e.g., 5 for business_5.txt)"
  echo "  BUSINESS_NUM2: Second business file number (e.g., 10 for business_10.txt)"
  exit 2
fi

# Read hosts from hosts.txt
if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "Error: $HOSTS_FILE not found"
  exit 1
fi

mapfile -t HOSTS < "$HOSTS_FILE"

# Validate VM number
if [[ "$VM_NUM" -lt 1 || "$VM_NUM" -gt "${#HOSTS[@]}" ]]; then
  echo "Error: VM_NUM must be between 1 and ${#HOSTS[@]}"
  exit 1
fi

CLIENT_VM="${HOSTS[$((VM_NUM - 1))]}"
LOCAL1="./business/business_${BUSINESS_NUM1}.txt"
LOCAL2="./business/business_${BUSINESS_NUM2}.txt"

# Validate business files exist
if [[ ! -f "$LOCAL1" ]]; then
  echo "Error: Business file not found: $LOCAL1"
  exit 1
fi

if [[ ! -f "$LOCAL2" ]]; then
  echo "Error: Business file not found: $LOCAL2"
  exit 1
fi

# Helper to run client command on client VM
run_on() {
  local host="$1"; shift
  if [[ "$host" == "localhost" || "$host" == "127.0.0.1" ]]; then
    (cd /home/saik2/mp3-g02 && eval "$@")
  else
    ssh -o LogLevel=ERROR "$host" "cd /home/saik2/mp3-g02 && $*"
  fi
}

# Step 1: Append LOCAL1 then LOCAL2 sequentially, from same client VM
echo "== Test 4: appends from $CLIENT_VM to $HYDFSFILE =="
run_on "$CLIENT_VM" "$CLIENT -cmd append $LOCAL1 $HYDFSFILE"
echo "-> First append finished"
run_on "$CLIENT_VM" "$CLIENT -cmd append $LOCAL2 $HYDFSFILE"
echo "-> Second append finished"

# Step 2: Immediately get the file from same client and look for markers
echo "\n== Fetching file to verify read-my-writes and ordering =="
if [[ "$CLIENT_VM" == "localhost" ]]; then
  $CLIENT -cmd get "$HYDFSFILE" "$TMP_OUT"
else
  ssh -o LogLevel=ERROR "$CLIENT_VM" "cd $(pwd) && $CLIENT -cmd get '$HYDFSFILE' '$TMP_OUT'"
fi

# Grep for snippets from local files to demonstrate ordering
echo "\n---- Search for markers from the appended files ----"
if [[ "$CLIENT_VM" == "localhost" ]]; then
  echo "Occurrences from $LOCAL1:"
  grep -n "$(head -n 1 "$LOCAL1" | sed 's/\//\\\//g')" "$TMP_OUT" || true
  echo "Occurrences from $LOCAL2:"
  grep -n "$(head -n 1 "$LOCAL2" | sed 's/\//\\\//g')" "$TMP_OUT" || true
else
  # On remote, copy result locally for convenience
  scp -o LogLevel=ERROR "$CLIENT_VM:$TMP_OUT" "/tmp/hy_get_after_appends.$CLIENT_VM"
  echo "Occurrences from $LOCAL1:"
  grep -n "$(ssh -o LogLevel=ERROR "$CLIENT_VM" "head -n 1 '$LOCAL1' | sed 's/\//\\\\\//g'")" "/tmp/hy_get_after_appends.$CLIENT_VM" || true
fi

# Optionally print the file tail for visual ordering
if [[ "$CLIENT_VM" == "localhost" ]]; then
  echo "\n--- tail of fetched file ---"
  tail -n 200 "$TMP_OUT"
else
  ssh -o LogLevel=ERROR "$CLIENT_VM" "tail -n 200 /tmp/hy_get_after_appends"
fi

echo "\n== Test 4 completed =="
exit 0
