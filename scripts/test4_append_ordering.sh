#!/usr/bin/env bash
# Test 4: Read-my-writes + client-append ordering
# Usage: ./test4_append_ordering.sh CLIENT_VM HYDFSFILE LOCAL1 LOCAL2
# Example: ./test4_append_ordering.sh vm1 demo_foo.txt local/foo1.txt local/foo2.txt

set -euo pipefail

CLIENT_VM=${1:-localhost}
HYDFSFILE=${2:-}
LOCAL1=${3:-}
LOCAL2=${4:-}
FILE_NAME="RandomFile1"
CLIENT="./home/saik2/mp3-g02/client"
TMP_OUT="/tmp/hy_get_after_appends"

if [[ -z "$HYDFSFILE" || -z "$LOCAL1" || -z "$LOCAL2" ]]; then
  echo "Usage: $0 CLIENT_VM HYDFSFILE LOCAL1 LOCAL2"
  exit 2
fi

# Helper to run client command on client VM
run_on() {
  local host="$1"; shift
  if [[ "$host" == "localhost" || "$host" == "127.0.0.1" ]]; then
    (cd /home/saik2/mp3-g02 && "$@")
  else
    ssh "$host" "cd /home/saik2/mp3-g02 && $*"
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
  ssh "$CLIENT_VM" "cd $(pwd) && $CLIENT -cmd get '$HYDFSFILE' '$TMP_OUT'"
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
  scp "$CLIENT_VM:$TMP_OUT" "/tmp/hy_get_after_appends.$CLIENT_VM"
  echo "Occurrences from $LOCAL1:"
  grep -n "$(ssh "$CLIENT_VM" "head -n 1 '$LOCAL1' | sed 's/\//\\\\\//g'")" "/tmp/hy_get_after_appends.$CLIENT_VM" || true
fi

# Optionally print the file tail for visual ordering
if [[ "$CLIENT_VM" == "localhost" ]]; then
  echo "\n--- tail of fetched file ---"
  tail -n 200 "$TMP_OUT"
else
  ssh "$CLIENT_VM" "tail -n 200 /tmp/hy_get_after_appends"
fi

echo "\n== Test 4 completed =="
exit 0
