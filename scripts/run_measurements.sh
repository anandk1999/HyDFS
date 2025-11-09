#!/bin/bash

# Master script to run all measurements
# 
# IMPORTANT: This script must be run ON ONE OF THE VMs, not on your local machine!
# Example: ssh to fa25-cs425-0201.cs.illinois.edu and run this from ~/mp3-g02
#

echo "Starting all measurements..."
echo "NOTE: Ensure you are running this ON A VM in the ~/mp3-g02 directory"
echo ""

# Check if ifstat is installed
if ! command -v ifstat &> /dev/null
then
    echo "ifstat could not be found. Please install it to continue."
    echo "On macOS, you can install it with: brew install ifstat"
    echo "On Debian/Ubuntu, you can install it with: sudo apt-get install ifstat"
    exit 1
fi

echo "Starting all measurements..."

# Ensure all scripts are executable
chmod +x measurements/*.sh
chmod +x scripts/*.sh

# --- Measurement 1: Re-replication Overhead ---
echo "--- Running Re-replication Overhead Measurement ---"
# Requires a 4-node cluster running
./scripts/cluster.sh start 4
sleep 10 # Wait for cluster to stabilize
./measurements/re-replication_overhead.sh
./scripts/cluster.sh stop
echo "--- Re-replication Measurement Complete ---"

# --- Measurement 2: Rebalancing Overhead ---
echo "--- Running Rebalancing Overhead Measurement ---"
# Requires a 3-node cluster initially
./scripts/cluster.sh start 3
sleep 10
./measurements/rebalancing_overhead.sh
./scripts/cluster.sh stop
echo "--- Rebalancing Measurement Complete ---"

# --- Measurement 3: Merge Performance ---
echo "--- Running Merge Performance Measurement ---"
# Requires a 4-node cluster
./scripts/cluster.sh start 4
sleep 10
./measurements/merge_performance.sh
./scripts/cluster.sh stop
echo "--- Merge Performance Measurement Complete ---"

# --- Measurement 4: Subsequent Merge Performance ---
echo "--- Running Subsequent Merge Performance Measurement ---"
# Requires a 4-node cluster
./scripts/cluster.sh start 4
sleep 10
./measurements/subsequent_merge_performance.sh
./scripts/cluster.sh stop
echo "--- Subsequent Merge Performance Measurement Complete ---"


echo "All measurements have been completed."
echo "You can find the results in the 'measurements/' subdirectories."
echo "Please remember to analyze the data and generate plots for your report."
