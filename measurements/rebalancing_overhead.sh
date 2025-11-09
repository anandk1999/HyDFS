#!/bin/bash

# Script to measure rebalancing overhead
#
# IMPORTANT: Run this script ON A VM (e.g., fa25-cs425-0201) from the ~/mp3-g02 directory
# NOT on your local machine!
#
# This measures the BANDWIDTH used when a new node joins and files are redistributed

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
REMOTE_DIR="mp3-g02"

echo "=== Rebalancing Bandwidth Measurement ==="
echo "This measures network bandwidth when node 4 joins and files are redistributed"
echo ""

for count in "${FILE_COUNTS[@]}"; do
    echo "========================================="
    echo "Measuring rebalancing for $count files of ${FILE_SIZE} bytes each"
    echo "========================================="

    # 1. Preload the system with $count files of size 128KB on 3-node cluster
    echo "[Step 1/5] Preloading $count files into 3-node cluster..."
    for i in $(seq 1 $count); do
        dd if=/dev/urandom of=testfile.tmp bs=$FILE_SIZE count=1 &>/dev/null
        ./client -cmd create testfile.tmp sdfs_rebalance_${count}_${i} &>/dev/null
        if [ $((i % 20)) -eq 0 ]; then
            echo "  Created $i/$count files..."
        fi
    done
    rm testfile.tmp
    echo "  ✓ Preload complete: $count files stored on 3 nodes"
    sleep 2

    # 2. Get introducer info
    echo "[Step 2/5] Getting introducer information..."
    INTRODUCER_HOST="${HOSTS[0]}"
    INTRODUCER_IP=$(ssh "${INTRODUCER_HOST}" "hostname -i" | tr -d '[:space:]')
    INTRODUCER_ADDR="${INTRODUCER_IP}:8080"
    echo "  Introducer: ${INTRODUCER_ADDR}"

    # 3. Start bandwidth monitoring on the NEW node (node 4) - it will receive files
    echo "[Step 3/5] Starting bandwidth monitoring on node 4 (${NODE_TO_ADD})..."
    
    # Start ifstat on node 4 to measure incoming bandwidth
    ssh "${NODE_TO_ADD}" "
        pkill -f ifstat 2>/dev/null || true
        sleep 1
        cd ${REMOTE_DIR}
        ifstat -i eth0 -d 1 -n > ${OUTPUT_DIR}/bandwidth_${count}.log 2>${OUTPUT_DIR}/ifstat_error_${count}.log &
        echo \$! > /tmp/ifstat.pid
    "
    
    sleep 2
    echo "  ✓ Bandwidth monitoring started on node 4"

    # 4. Start node 4 to trigger rebalancing
    echo "[Step 4/5] Starting node 4 to join cluster and trigger rebalancing..."
    NODE_PORT=8083
    NODE_CONTROL_PORT=18080
    
    ssh "${NODE_TO_ADD}" "
        cd ${REMOTE_DIR}
        pkill -f './client -port' 2>/dev/null || true
        rm -rf hydfs_storage
        nohup ./client -port ${NODE_PORT} -control-port ${NODE_CONTROL_PORT} -introducer ${INTRODUCER_ADDR} > node4.log 2>&1 &
    "
    
    echo "  ✓ Node 4 joining cluster..."
    
    # 5. Wait for rebalancing to complete
    echo "[Step 5/5] Waiting for rebalancing (60 seconds)..."
    sleep 60
    
    # Stop bandwidth monitoring
    echo "  Stopping bandwidth monitoring..."
    ssh "${NODE_TO_ADD}" "
        if [ -f /tmp/ifstat.pid ]; then
            kill \$(cat /tmp/ifstat.pid) 2>/dev/null || true
            rm /tmp/ifstat.pid
        fi
        pkill -f ifstat 2>/dev/null || true
    "
    
    # Copy bandwidth log back to this machine
    scp "${NODE_TO_ADD}:~/${REMOTE_DIR}/${OUTPUT_DIR}/bandwidth_${count}.log" "$OUTPUT_DIR/" &>/dev/null
    
    echo "  ✓ Measurement complete for $count files"
    
    # Stop node 4 to reset for next iteration
    echo "  Stopping node 4 for next iteration..."
    ssh "${NODE_TO_ADD}" "pkill -f client"
    sleep 5
    
    echo ""
done

echo "========================================="
echo "✓ All rebalancing measurements complete!"
echo "Results are in: $OUTPUT_DIR"
echo "========================================="
