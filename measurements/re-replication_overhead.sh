#!/bin/bash

# Script to measure re-replication overhead
#
# IMPORTANT: Run this script ON A VM (e.g., fa25-cs425-0201) from the ~/mp3-g02 directory
# NOT on your local machine!
#

# File sizes in bytes (1KB, 10KB, 100KB, 512KB, 1MB)
FILE_SIZES=(1024 10240 102400 524288 1048576)
NUM_FILES=100
OUTPUT_DIR="measurements/re-replication"
mkdir -p $OUTPUT_DIR

# Read hosts into an array
HOSTS=()
while IFS= read -r line; do
    HOSTS+=("$line")
done < ./hosts.txt
NODE_TO_KILL="${HOSTS[3]}" # Kill the 4th node in the list
REMOTE_DIR="mp3-g02"  # Assuming project is in ~/mp3-g02 on remote machines

# Assuming the cluster is already running with 4 nodes
# And this script is run from the root of the project directory

for size in "${FILE_SIZES[@]}"; do
    echo "Measuring re-replication for file size: $size bytes"
    
    # 1. Preload the system with 100 files of the current size
    echo "Preloading 100 files of size $size..."
    for i in $(seq 1 $NUM_FILES); do
        dd if=/dev/urandom of=testfile.tmp bs=$size count=1 &>/dev/null
        ./client put testfile.tmp sdfs_testfile_${size}_${i} &>/dev/null
    done
    rm testfile.tmp
    echo "Preload complete."

    # 2. Kill one of the nodes
    echo "Killing node ${NODE_TO_KILL}..."
    ssh "${NODE_TO_KILL}" "pkill -f client" &

    # 3. Measure re-replication time and bandwidth
    # Kill any existing ifstat processes first
    pkill -f ifstat 2>/dev/null || true
    sleep 1
    
    # Start measuring bandwidth (sample every 1 second)
    ifstat -d 1 -n > $OUTPUT_DIR/bandwidth_${size}.log &
    ifstat_pid=$!

    start_time=$(date +%s%N)

    # Wait for re-replication to complete by checking if files are properly replicated
    # We'll check the ls output to verify all 100 files are back to 3 replicas
    echo "Waiting for re-replication to complete..."
    
    MAX_WAIT=120  # Maximum wait time in seconds
    CHECK_INTERVAL=2  # Check every 2 seconds
    elapsed=0
    
    while [ $elapsed -lt $MAX_WAIT ]; do
        # Check if re-replication is complete by checking one of the files
        # The ls command should show 3 replica node addresses
        test_file="sdfs_testfile_${size}_1"
        ls_output=$(ssh "${HOSTS[0]}" "cd ${REMOTE_DIR}; ./client -control-port 18080 -cmd ls ${test_file}" 2>/dev/null)
        
        # Count the number of node addresses in the output (lines with :8080)
        replica_count=$(echo "$ls_output" | grep -c ":8080" 2>/dev/null || echo "0")
        
        # If we see 3 replicas for our test file, re-replication is likely complete
        if [ "$replica_count" -eq 3 ] 2>/dev/null; then
            echo "Re-replication complete! Test file has 3 replicas."
            break
        fi
        
        sleep $CHECK_INTERVAL
        elapsed=$((elapsed + CHECK_INTERVAL))
    done
    
    if [ $elapsed -ge $MAX_WAIT ]; then
        echo "Warning: Re-replication timeout after ${MAX_WAIT}s"
    fi

    end_time=$(date +%s%N)
    
    # Stop measuring bandwidth
    kill $ifstat_pid 2>/dev/null || true

    elapsed_time=$((($end_time - $start_time) / 1000000)) # in milliseconds
    echo "Re-replication time for file size $size: $elapsed_time ms" > $OUTPUT_DIR/time_${size}.log

    echo "Measurement for file size $size complete."

    # Restart the killed node to bring the cluster back to a healthy state
    echo "Restarting node ${NODE_TO_KILL}..."
    ssh "${NODE_TO_KILL}" "cd ${REMOTE_DIR}; ./client" &
    sleep 5 # Give it time to rejoin
done

echo "All re-replication overhead measurements complete."
echo "Results are in the $OUTPUT_DIR directory."
