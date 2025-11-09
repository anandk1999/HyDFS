#!/bin/bash

# Script to measure merge performance
#
# IMPORTANT: Run this script ON A VM (e.g., fa25-cs425-0201) from the ~/mp3-g02 directory
# NOT on your local machine!
#

CONCURRENT_CLIENTS=(1 2 5 10)
APPEND_SIZES=(4096 32768) # 4KiB and 32KiB
INITIAL_FILE_SIZE=131072 # 128KiB
NUM_READINGS=5
OUTPUT_DIR="measurements/merge"
mkdir -p $OUTPUT_DIR

# Read hosts into an array
HOSTS=()
while IFS= read -r line; do
    HOSTS+=("$line")
done < ./hosts.txt
REMOTE_DIR="mp3-g02"  # Assuming project is in ~/mp3-g02 on remote machines

# Assuming the cluster is running
# And this script is run from the root of the project directory

for append_size in "${APPEND_SIZES[@]}"; do
    for num_clients in "${CONCURRENT_CLIENTS[@]}"; do
        echo "Measuring merge performance for $num_clients clients with append size $append_size"
        
        total_time=0
        for i in $(seq 1 $NUM_READINGS); do
            # 1. Create and put the initial file
            dd if=/dev/urandom of=initial_file.tmp bs=$INITIAL_FILE_SIZE count=1 &>/dev/null
            ./client -cmd create initial_file.tmp sdfs_merge_test &>/dev/null
            rm initial_file.tmp

            # 2. Create the data to be appended
            dd if=/dev/urandom of=append_data.tmp bs=$append_size count=1 &>/dev/null

            # 3. Perform concurrent appends from different VMs
            pids=()
            for j in $(seq 1 $num_clients); do
                node_num=$(( (j - 1) % 4 )) # Cycle through nodes 0-3
                node="${HOSTS[$node_num]}"
                scp append_data.tmp $node:~/append_data.tmp &>/dev/null
                ssh $node "cd ${REMOTE_DIR}; ./client -cmd append ~/append_data.tmp sdfs_merge_test" &
                pids+=($!)
            done

            # Wait for all appends to complete
            for pid in "${pids[@]}"; do
                wait $pid
            done
            
            rm append_data.tmp

            # 4. Measure merge time
            start_time=$(date +%s%N)
            ./client -cmd merge sdfs_merge_test &>/dev/null
            end_time=$(date +%s%N)

            elapsed_time=$((($end_time - $start_time) / 1000000)) # in milliseconds
            total_time=$(($total_time + $elapsed_time))

            # 5. Note: No delete command - file will remain, but will be overwritten in next iteration
            sleep 2
        done

        avg_time=$(($total_time / $NUM_READINGS))
        echo "Average merge time for $num_clients clients, append size $append_size: $avg_time ms" > $OUTPUT_DIR/merge_${append_size}_${num_clients}.log
    done
done

echo "All merge performance measurements complete."
echo "Results are in the $OUTPUT_DIR directory."
