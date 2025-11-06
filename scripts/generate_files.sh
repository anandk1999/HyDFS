#!/bin/bash

# ==============================================================================
# HyDFS Test Data Generation Script
#
# This script creates a 'testdata' directory in the project root and populates
# it with files of various sizes and content types needed for functionality
# and performance testing as per the MP3 spec.
# ==============================================================================

# --- Configuration ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
TEST_DATA_DIR="${PROJECT_ROOT}/testdata"

# --- Colors for better output ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Creating test data directory at: ${TEST_DATA_DIR}${NC}"
mkdir -p "${TEST_DATA_DIR}"

# --- 1. Small, Human-Readable Files for Basic Functionality ---
echo -e "\n${CYAN}Generating small text files for basic operations...${NC}"

echo "Hello, Distributed File System!" > "${TEST_DATA_DIR}/hello_dfs.txt"
echo "This is the first piece of data to be appended." > "${TEST_DATA_DIR}/append_A.txt"
echo "This is the second piece of data, which must appear after the first." > "${TEST_DATA_DIR}/append_B.txt"

# --- 2. Unique Files for Concurrency (multiappend) Test ---
echo -e "\n${CYAN}Generating unique client files for concurrency tests...${NC}"
for i in {1..10}; do
    content="--- DATA BLOCK FROM CLIENT VM ${i} -- Timestamp: $(date +%s) ---"
    echo "${content}" > "${TEST_DATA_DIR}/client_${i}_data.txt"
done

# --- 3. Files for Performance Measurement ---
echo -e "\n${CYAN}Generating binary files for performance measurements...${NC}"

# Function to generate a binary file of a specific size using dd for portability
generate_binary_file() {
    local size=$1
    local unit=$2
    local file_path=$3
    local byte_size=0

    # Convert K/M suffix to a pure byte count
    case "$unit" in
        K) byte_size=$((size * 1024)) ;;
        M) byte_size=$((size * 1024 * 1024)) ;;
        *)
          echo "Error: Unknown unit '$unit'"
          return 1
          ;;
    esac

    echo "Creating ${file_path} (${size}${unit} -> ${byte_size} bytes)..."
    
    # Use dd for cross-platform compatibility (works on macOS and Linux)
    # Suppress the status output from dd
    dd if=/dev/urandom of="${file_path}" bs=${byte_size} count=1 >/dev/null 2>&1
}

# For merge performance tests (Append sizes)
generate_binary_file 4 K "${TEST_DATA_DIR}/append_4K.bin"
generate_binary_file 32 K "${TEST_DATA_DIR}/append_32K.bin"

# For rebalancing overhead tests (Initial file size)
generate_binary_file 128 K "${TEST_DATA_DIR}/initial_128K.bin"

# For re-replication overhead tests (Varying file sizes up to 1MiB)
generate_binary_file 100 K "${TEST_DATA_DIR}/file_100K.bin"
generate_binary_file 250 K "${TEST_DATA_DIR}/file_250K.bin"
generate_binary_file 500 K "${TEST_DATA_DIR}/file_500K.bin"
generate_binary_file 750 K "${TEST_DATA_DIR}/file_750K.bin"
generate_binary_file 1 M "${TEST_DATA_DIR}/file_1M.bin"

echo -e "\n${GREEN}Test data generation complete.${NC}"
ls -lh "${TEST_DATA_DIR}"