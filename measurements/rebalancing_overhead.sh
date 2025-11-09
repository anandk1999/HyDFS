#!/bin/bash

# Script to measure rebalancing overhead
#
# IMPORTANT: Run this script ON A VM from the ~/mp3-g02 directory
#
# WHAT THIS MEASURES:
# When a 4th node joins a 3-node cluster, approximately 1/4 of the files need to 
# move to the new node based on consistent hashing. We measure:
# 1. Time for rebalancing to complete
# 2. Total data transferred (file_count * file_size * 0.25)

FILE_COUNTS=(10 50 100 200)
FILE_SIZE=131072 # 128 KiB
OUTPUT_DIR="measurements/rebalancing"
mkdir -p $OUTPUT_DIR

# Read hosts into an array
HOSTS=()
while IFS= read -r line; do
    HOSTS+=("$line")
done < ./hosts.txt

NODE_TO_ADD="${HOSTS[3]}" # 4th node

echo "=== Rebalancing Overhead Measurement ==="
echo "Cluster: 3 nodes initially, adding 4th node"
echo ""

for count in "${FILE_COUNTS[@]}"; do
    echo "========================================="
    echo "Test: $count files × ${FILE_SIZE} bytes"
    echo "========================================="

    # 1. Preload files on 3-node cluster
    echo "[1/4] Creating $count files on 3-node cluster..."
    for i in $(seq 1 $count); do
        dd if=/dev/urandom of=testfile_${i}.tmp bs=$FILE_SIZE count=1 &>/dev/null
        ./client -cmd create testfile_${i}.tmp rebal_file_${i} &>/dev/null
        rm testfile_${i}.tmp
        
        if [ $((i % 10)) -eq 0 ]; then
            echo "  Created $i/$count files"
        fi
    done
    echo "  ✓ All $count files created and replicated on 3 nodes"
    sleep 3

    # 2. Get cluster info
    INTRODUCER_HOST="${HOSTS[0]}"
    INTRODUCER_IP=$(ssh "${INTRODUCER_HOST}" "hostname -i" | tr -d '[:space:]')
    INTRODUCER_ADDR="${INTRODUCER_IP}:8080"

    # 3. Join 4th node and measure time
    echo "[2/4] Adding 4th node to trigger rebalancing..."
    
    # Clean node 4 first
    ssh "${NODE_TO_ADD}" "
        cd mp3-g02
        pkill -f './client -port' 2>/dev/null || true
        rm -rf hydfs_storage
    " 2>/dev/null

    # Start timer
    start_time=$(date +%s)
    
    # Start node 4
    ssh "${NODE_TO_ADD}" "
        cd mp3-g02
        nohup ./client -port 8083 -control-port 18080 -introducer ${INTRODUCER_ADDR} > node4_rebal.log 2>&1 &
    " 2>/dev/null
    
    echo "  Node 4 started, waiting for rebalancing..."

    # 4. Wait and detect when rebalancing completes
    echo "[3/4] Monitoring rebalancing progress..."
    
    max_wait=120
    elapsed=0
    check_interval=5
    
    while [ $elapsed -lt $max_wait ]; do
        # Check how many files node 4 has received
        files_on_node4=$(ssh "${NODE_TO_ADD}" "
            cd mp3-g02
            ./client -cmd liststore 2>/dev/null | grep -c 'rebal_file_' || echo 0
        " 2>/dev/null)
        
        echo "  [${elapsed}s] Node 4 has $files_on_node4 files"
        
        # Expect roughly 25% of files on node 4 (could vary due to hashing)
        expected_min=$((count / 5))  # At least 20% 
        
        if [ "$files_on_node4" -ge "$expected_min" ]; then
            echo "  ✓ Rebalancing appears complete!"
            break
        fi
        
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
    done
    
    end_time=$(date +%s)
    rebalance_time=$((end_time - start_time))
    
    # Calculate theoretical data transferred (assume 25% of files moved to new node)
    total_data_bytes=$((count * FILE_SIZE))
    transferred_bytes=$((total_data_bytes / 4))
    transferred_mb=$((transferred_bytes / 1024 / 1024))
    
    echo "[4/4] Recording results..."
    echo "  Rebalancing time: ${rebalance_time}s"
    echo "  Files on node 4: $files_on_node4"
    echo "  Theoretical data transferred: ${transferred_mb} MB"
    
    # Save results
    {
        echo "File count: $count"
        echo "File size: ${FILE_SIZE} bytes"
        echo "Rebalancing time: ${rebalance_time} seconds"
        echo "Files on node 4: $files_on_node4"
        echo "Theoretical data transferred: ${transferred_mb} MB"
        echo "Average bandwidth: $((transferred_mb / rebalance_time)) MB/s (theoretical)"
    } > "$OUTPUT_DIR/rebalance_${count}.log"
    
    # Also save just the bandwidth for plotting
    bandwidth_mbps=$((transferred_mb / rebalance_time))
    echo "$bandwidth_mbps" > "$OUTPUT_DIR/bandwidth_${count}.log"
    
    echo "  ✓ Results saved"
    
    # Stop node 4 for next iteration
    echo "  Cleaning up for next test..."
    ssh "${NODE_TO_ADD}" "pkill -f client" 2>/dev/null
    sleep 3
    
    echo ""
done

echo "========================================="
echo "✓ All measurements complete!"
echo "Results in: $OUTPUT_DIR"
echo "========================================="
