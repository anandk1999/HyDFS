#!/bin/bash

# Script to measure rebalancing overhead
#
# IMPORTANT: Run this script ON A VM (e.g., fa25-cs425-0201) from the ~/mp3-g02 directory
# NOT on your local machine!
#

FILE_COUNTS=(10 50 100 200)
FILE_SIZE=131072 # 128KiB
OUTPUT_DIR="measurements/rebalancing"
mkdir -p $OUTPUT_DIR

# Read hosts into an array
HOSTS=()
while IFS= read -r line; do
    HOSTS+=("$line")
done < ./hosts.txt
NODE_TO_ADD="${HOSTS[3]}" # The 4th node in the list
REMOTE_DIR="mp3-g02"  # Assuming project is in ~/mp3-g02 on remote machines

# Assuming the cluster is running with 3 nodes initially
# And this script is run from the root of the project directory

for count in "${FILE_COUNTS[@]}"; do
    echo "Measuring rebalancing for $count files"

    # 1. Preload the system with $count files of size 128KB
    echo "Preloading $count files..."
    for i in $(seq 1 $count); do
        dd if=/dev/urandom of=testfile.tmp bs=$FILE_SIZE count=1 &>/dev/null
        ./client -cmd create testfile.tmp sdfs_testfile_rebalance_${count}_${i} &>/dev/null
    done
    rm testfile.tmp
    echo "Preload complete."

    # 2. Start the 4th node to trigger rebalancing
    echo "Starting node ${NODE_TO_ADD} to trigger rebalancing..."
    ssh "${NODE_TO_ADD}" "cd ${REMOTE_DIR}; ./client" &
    
    # 3. Measure bandwidth (sample every 1 second)
    # Kill any existing ifstat processes first
    pkill -f ifstat 2>/dev/null || true
    sleep 1
    
    ifstat -d 1 -n > $OUTPUT_DIR/bandwidth_${count}.log &
    ifstat_pid=$!

    # Wait for rebalancing to complete.
    # This is tricky. We'll wait for a fixed time.
    echo "Waiting for rebalancing to complete..."
    sleep 5 # Adjust as needed

    # Stop measuring bandwidth
    kill $ifstat_pid 2>/dev/null || true

    echo "Measurement for $count files complete."

    # Stop the 4th node to reset for the next run
    echo "Stopping node ${NODE_TO_ADD}..."
    ssh "${NODE_TO_ADD}" "pkill -f client"
    sleep 5

    # Note: No delete command available - files will remain in HyDFS
    # You may need to manually clean up or restart the cluster between runs

done

echo "All rebalancing overhead measurements complete."
echo "Results are in the $OUTPUT_DIR directory."
