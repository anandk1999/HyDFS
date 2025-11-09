#!/bin/bash

# Script to measure subsequent merge performance
#
# IMPORTANT: Run this script ON A VM (e.g., fa25-cs425-0201) from the ~/mp3-g02 directory
# NOT on your local machine!
#

# Check if running on a VM (not local machine)
if [[ $(hostname) != *"cs425"* ]]; then
    echo "ERROR: This script must be run ON A VM (e.g., fa25-cs425-0201), not locally!"
    echo "Please SSH to a VM and run from ~/mp3-g02 directory."
    exit 1
fi

CONCURRENT_CLIENTS=(1 2 5 10)
APPEND_SIZES=(4096 32768) # 4KiB and 32KiB
INITIAL_FILE_SIZE=131072 # 128KiB
NUM_READINGS=5
OUTPUT_DIR="measurements/subsequent_merge"
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
        echo "Measuring subsequent merge for $num_clients clients with append size $append_size"
        
        total_time1=0
        total_time2=0
        for i in $(seq 1 $NUM_READINGS); do
            # 1. Create and put the initial file
            dd if=/dev/urandom of=initial_file.tmp bs=$INITIAL_FILE_SIZE count=1 &>/dev/null
            ./client -cmd create initial_file.tmp sdfs_sub_merge_test &>/dev/null
            rm initial_file.tmp

            # 2. Create the data to be appended on all VMs
            dd if=/dev/urandom of=append_data.tmp bs=$append_size count=1 &>/dev/null
            
            # Copy append data to all VMs that will participate
            for j in $(seq 1 $num_clients); do
                node_num=$(( (j - 1) % 4 ))
                node="${HOSTS[$node_num]}"
                scp append_data.tmp $node:~/append_data_${j}.tmp &>/dev/null
            done

            # 3. Build multiappend command arguments: HyDFSfile VM1 localfile1 VM2 localfile2 ...
            multiappend_args="sdfs_sub_merge_test"
            for j in $(seq 1 $num_clients); do
                node_num=$(( (j - 1) % 4 ))
                node="${HOSTS[$node_num]}"
                multiappend_args="$multiappend_args $node ~/append_data_${j}.tmp"
            done

            # 4. Perform concurrent appends using multiappend
            ./client -cmd multiappend $multiappend_args &>/dev/null
            
            rm append_data.tmp

            # 5. First merge
            start_time1=$(date +%s%N)
            ./client -cmd merge sdfs_sub_merge_test &>/dev/null
            end_time1=$(date +%s%N)
            elapsed_time1=$((($end_time1 - $start_time1) / 1000000))
            total_time1=$(($total_time1 + $elapsed_time1))

            # 6. Second merge
            start_time2=$(date +%s%N)
            ./client -cmd merge sdfs_sub_merge_test &>/dev/null
            end_time2=$(date +%s%N)
            elapsed_time2=$((($end_time2 - $start_time2) / 1000000))
            total_time2=$(($total_time2 + $elapsed_time2))

            # 7. Note: No delete command - file will remain, but will be overwritten in next iteration
            sleep 2
        done

        avg_time1=$(($total_time1 / $NUM_READINGS))
        avg_time2=$(($total_time2 / $NUM_READINGS))
        
        echo "Average first merge time: $avg_time1 ms" > $OUTPUT_DIR/sub_merge_${append_size}_${num_clients}.log
        echo "Average second merge time: $avg_time2 ms" >> $OUTPUT_DIR/sub_merge_${append_size}_${num_clients}.log
    done
done

echo "All subsequent merge performance measurements complete."
echo "Results are in the $OUTPUT_DIR directory."
