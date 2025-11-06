# HyDFS Comprehensive Test Commands for 10 VMs

## Prerequisites Setup

```bash
# Generate test files on VM1 (or your local machine, then transfer)
cd ~/mp3-g02
./scripts/generate_files.sh

# Verify test files exist
ls -lh testdata/
```

## Phase 1: Basic Cluster Setup and Membership

### Start all 10 VMs
```bash
# From your local machine
./scripts/cluster.sh start
```

### Verify membership on each VM
```bash
# On VM1
./client -cmd list_mem

# On VM1 - verify ring IDs
./client -cmd list_mem_ids

# Should show all 10 nodes as Alive with their Ring IDs sorted
```

### Test MP1 log querying (verify logging works)
```bash
# On VM1
./client -cmd grep_logs "HyDFS Server started"

# Should return log entries from all 10 VMs
```

## Phase 2: Basic File Operations

### Test 1: Create a file
```bash
# On VM1
./client -cmd create testdata/hello_dfs.txt test_file_1.txt

# Expected output: Shows 3 replica VMs, FileID (hex string), W=3
# Should show: "File test_file_1.txt created successfully on replicas (W=3 & FileID=<hex>)"
```

### Test 2: List file replicas
```bash
# On VM1
./client -cmd ls test_file_1.txt

# Expected: Shows 3 VMs with their Ring IDs
```

### Test 3: List local storage on replicas
```bash
# On the 3 replica VMs shown above
./client -cmd liststore

# Expected: Should show test_file_1.txt with its FileID
```

### Test 4: Get file
```bash
# On VM2 (different from VM1)
./client -cmd get test_file_1.txt downloaded_test1.txt

# Verify content
cat downloaded_test1.txt
# Should match: "Hello, Distributed File System!"
```

### Test 5: Append to file
```bash
# On VM3
./client -cmd append testdata/hello_dfs.txt test_file_1.txt

# Expected: Shows replicas, W=2 (eventual consistency)

# Get and verify
./client -cmd get test_file_1.txt downloaded_test1_after_append.txt
cat downloaded_test1_after_append.txt
# Should show content twice
```

## Phase 3: Client Identity and Per-Client Ordering

### Test 6: Concurrent appends from different clients
```bash
# On VM1 - append chunk 1
echo "VM1_append_1" > /tmp/vm1_chunk1.txt
./client -cmd append /tmp/vm1_chunk1.txt test_client_order.txt 2>&1 | grep "created\|appended"

# On VM2 - append chunk 2 (within same time window)
echo "VM2_append_1" > /tmp/vm2_chunk1.txt
./client -cmd append /tmp/vm2_chunk1.txt test_client_order.txt 2>&1

# On VM1 - append chunk 2
echo "VM1_append_2" > /tmp/vm1_chunk2.txt
./client -cmd append /tmp/vm1_chunk2.txt test_client_order.txt 2>&1

# On VM2 - append chunk 2
echo "VM2_append_2" > /tmp/vm2_chunk2.txt
./client -cmd append /tmp/vm2_chunk2.txt test_client_order.txt 2>&1

# Get and verify ordering
./client -cmd get test_client_order.txt /tmp/client_order_test.txt
cat /tmp/client_order_test.txt

# Expected: VM1 appends should be in order (VM1_append_1 before VM1_append_2)
#           VM2 appends should be in order (VM2_append_1 before VM2_append_2)
```

## Phase 4: Multiappend (Concurrent Appends)

### Test 7: Launch concurrent appends
```bash
# Create test files on each VM first
# On VM1-VM5
for i in {1..5}; do
  ssh fa25-cs425-020$i.cs.illinois.edu "echo 'Append from VM$i' > ./mp3-g02/tmp/append_vm$i.txt"
done

# On VM1 - launch multiappend
./client -cmd multiappend test1_multi.txt \
  fa25-cs425-0201.cs.illinois.edu /tmp/append_vm1.txt \
  fa25-cs425-0202.cs.illinois.edu /tmp/append_vm2.txt \
  fa25-cs425-0203.cs.illinois.edu /tmp/append_vm3.txt \
  fa25-cs425-0204.cs.illinois.edu /tmp/append_vm4.txt \
  fa25-cs425-0205.cs.illinois.edu /tmp/append_vm5.txt

# Verify
./client -cmd get test1_multi.txt /tmp/multi_result1.txt
cat /tmp/multi_result1.txt
# Should show all 5 appends
```

## Phase 5: Merge Operations

### Test 8: Verify divergence and merge
```bash
# On VM1 - create file
./client -cmd create testdata/hello_dfs.txt test_merge.txt

# Stop one replica temporarily (find which VMs have it)
./client -cmd ls test_merge.txt
# Note the 3 replica VMs, let's say VM3, VM5, VM7

# On VM3 - stop the daemon temporarily
ssh fa25-cs425-0203.cs.illinois.edu "pkill -f 'go run main.go'"

# On VM1 - append while VM3 is down (W=2 should succeed)
echo "Append while VM3 down" > /tmp/append_during_failure.txt
./client -cmd append /tmp/append_during_failure.txt test_merge.txt

# On VM4 - append from different client
echo "Append from VM4" > /tmp/append_vm4.txt
ssh fa25-cs425-0204.cs.illinois.edu "cd ~/mp3-g02 && ./client -cmd append /tmp/append_vm4.txt test_merge.txt"

# Restart VM3
ssh fa25-cs425-0203.cs.illinois.edu "cd ~/mp3-g02 && nohup go run main.go -port 8080 &"

# Wait for VM3 to rejoin (check membership)
sleep 5
./client -cmd list_mem

# Check for divergence - get from each replica
./client -cmd getfromreplica fa25-cs425-0205.cs.illinois.edu test_merge.txt /tmp/replica_vm5.txt
./client -cmd getfromreplica fa25-cs425-0207.cs.illinois.edu test_merge.txt /tmp/replica_vm7.txt
./client -cmd getfromreplica fa25-cs425-0203.cs.illinois.edu test_merge.txt /tmp/replica_vm3.txt

# Compare file sizes (VM3 might be behind)
wc -c /tmp/replica_*.txt

# Execute merge
./client -cmd merge test_merge.txt

# Verify all replicas are identical after merge
./client -cmd getfromreplica fa25-cs425-0205.cs.illinois.edu test_merge.txt /tmp/merged_vm5.txt
./client -cmd getfromreplica fa25-cs425-0207.cs.illinois.edu test_merge.txt /tmp/merged_vm7.txt
./client -cmd getfromreplica fa25-cs425-0203.cs.illinois.edu test_merge.txt /tmp/merged_vm3.txt

# All should be identical
md5sum /tmp/merged_*.txt
```

## Phase 6: Failure Handling and Re-replication

### Test 9: Single node failure
```bash
# On VM1 - create large file
./client -cmd create testdata/large_1mb.bin test_failure.bin

# Check initial replicas
./client -cmd ls test_failure.bin
# Note the 3 VMs, let's say VM2, VM4, VM6

# Kill VM4
ssh fa25-cs425-0204.cs.illinois.edu "pkill -f 'go run main.go'"

# Wait for failure detection (~10-15 seconds)
sleep 15

# Check membership
./client -cmd list_mem
# VM4 should show as Failed or not present

# Check logs for re-replication
./client -cmd grep_logs "ReReplication.*test_failure"

# Verify file is now on 3 alive nodes
./client -cmd ls test_failure.bin

# Verify we can still get the file
./client -cmd get test_failure.bin /tmp/after_failure.bin
md5sum testdata/large_1mb.bin /tmp/after_failure.bin
# Should match
```

### Test 10: Two simultaneous failures
```bash
# Create test file
./client -cmd create testdata/medium_128kb.bin test_two_failures.bin

# Check replicas
./client -cmd ls test_two_failures.bin
# Note 3 VMs

# Kill two VMs (from different replicas)
ssh fa25-cs425-0203.cs.illinois.edu "pkill -f 'go run main.go'"
ssh fa25-cs425-0205.cs.illinois.edu "pkill -f 'go run main.go'"

# Wait for detection and re-replication
sleep 20

# Verify membership
./client -cmd list_mem

# Check file is still accessible
./client -cmd get test_two_failures.bin /tmp/two_fail_result.bin

# Verify file on new replicas
./client -cmd ls test_two_failures.bin

# Restart failed VMs (they should rejoin with fresh storage)
ssh fa25-cs425-0203.cs.illinois.edu "cd ~/mp3-g02 && nohup go run main.go -port 8080 &"
ssh fa25-cs425-0205.cs.illinois.edu "cd ~/mp3-g02 && nohup go run main.go -port 8080 &"
```

## Phase 7: Node Join and Rebalancing

### Test 11: Dynamic node addition
```bash
# Create multiple files first (spread across ring)
for i in {1..10}; do
  ./client -cmd create testdata/hello_dfs.txt "rebalance_test_$i.txt"
done

# List where files are stored
for i in {1..10}; do
  ./client -cmd ls "rebalance_test_$i.txt"
done

# Stop VM10
ssh fa25-cs425-0210.cs.illinois.edu "pkill -f 'go run main.go'"

# Wait
sleep 10

# Start VM10 (it should trigger rebalancing)
ssh fa25-cs425-0210.cs.illinois.edu "cd ~/mp3-g02 && nohup go run main.go -port 8080 -introducer fa25-cs425-0201.cs.illinois.edu:8080 &"

# Wait for rebalancing
sleep 30

# Check logs for rebalancing activity
./client -cmd grep_logs "Rebalance.*completed"

# Verify files are on correct successors
for i in {1..10}; do
  ./client -cmd ls "rebalance_test_$i.txt"
done
```

## Phase 8: Stress Testing and Performance

### Test 12: Large file operations
```bash
# Create 1MB file
./client -cmd create testdata/large_1mb.bin large_test.bin
time ./client -cmd get large_test.bin /tmp/large_downloaded.bin

# Multiple appends
for i in {1..10}; do
  ./client -cmd append testdata/medium_128kb.bin large_test.bin
done

# Get and verify
./client -cmd get large_test.bin /tmp/large_final.bin
ls -lh /tmp/large_final.bin
# Should be ~2.28 MB (1MB + 10*128KB)
```

### Test 13: Many small files
```bash
# Create 50 small files
for i in {1..50}; do
  echo "Test file $i" > /tmp/small_$i.txt
  ./client -cmd create /tmp/small_$i.txt "small_file_$i.txt"
done

# List storage on all VMs
for i in {1..10}; do
  echo "=== VM $i ==="
  ssh fa25-cs425-020$i.cs.illinois.edu "cd ~/mp3-g02 && ./client -cmd liststore | grep 'Stored files:' -A 20"
done

# Verify distribution is balanced
```

## Phase 9: Read-My-Writes Guarantee

### Test 14: Immediate read after write
```bash
# On VM1 - create file
./client -cmd create testdata/hello_dfs.txt read_my_write.txt

# Immediately append and get (should see append)
./client -cmd append testdata/hello_dfs.txt read_my_write.txt
./client -cmd get read_my_write.txt /tmp/rmw_test.txt

# Verify content includes both creates
cat /tmp/rmw_test.txt
# Should see content twice
```

## Phase 10: Background Merge Verification

### Test 15: Verify automatic background merge
```bash
# Create file
./client -cmd create testdata/hello_dfs.txt bg_merge_test.txt

# Get initial replicas
./client -cmd ls bg_merge_test.txt

# Stop one replica, append, restart it
# (Similar to Test 8 but wait for background merge instead of explicit merge)

# Wait for background merge (check every 5 seconds, runs in background)
sleep 30

# Check logs
./client -cmd grep_logs "BackgroundMerge.*bg_merge_test"

# Verify replicas are identical without explicit merge
```

## Phase 11: Edge Cases

### Test 16: Create same file twice (should fail)
```bash
./client -cmd create testdata/hello_dfs.txt duplicate_test.txt
./client -cmd create testdata/hello_dfs.txt duplicate_test.txt
# Second create should fail
```

### Test 17: Append to non-existent file (should fail)
```bash
./client -cmd append testdata/hello_dfs.txt nonexistent_file.txt
# Should fail with appropriate error
```

### Test 18: Get non-existent file (should fail)
```bash
./client -cmd get nonexistent_file.txt /tmp/should_fail.txt
# Should fail with "not found" error
```

## Phase 12: Full System Test

### Test 19: Complete workflow
```bash
# 1. Create file on VM1
ssh fa25-cs425-0201.cs.illinois.edu "cd ~/mp3-g02 && ./client -cmd create testdata/hello_dfs.txt workflow_test.txt"

# 2. Append from VM2
ssh fa25-cs425-0202.cs.illinois.edu "cd ~/mp3-g02 && ./client -cmd append testdata/hello_dfs.txt workflow_test.txt"

# 3. Append from VM3
ssh fa25-cs425-0203.cs.illinois.edu "cd ~/mp3-g02 && ./client -cmd append testdata/hello_dfs.txt workflow_test.txt"

# 4. Get from VM4
ssh fa25-cs425-0204.cs.illinois.edu "cd ~/mp3-g02 && ./client -cmd get workflow_test.txt /tmp/workflow.txt"

# 5. Kill one replica
REPLICAS=$(ssh fa25-cs425-0201.cs.illinois.edu "cd ~/mp3-g02 && ./client -cmd ls workflow_test.txt")
# Kill one of them

# 6. Verify re-replication
sleep 20

# 7. Merge
ssh fa25-cs425-0205.cs.illinois.edu "cd ~/mp3-g02 && ./client -cmd merge workflow_test.txt"

# 8. Get from all new replicas and verify identical
```

## Cleanup

### Stop all VMs
```bash
./scripts/cluster.sh stop
```

### Clear storage for fresh start
```bash
./scripts/cluster.sh clean
```

## Log Analysis Commands

### Check all create operations
```bash
./client -cmd grep_logs "Received /create"
```

### Check all append operations
```bash
./client -cmd grep_logs "Received /append"
```

### Check all get operations
```bash
./client -cmd grep_logs "Received /get"
```

### Check merge operations
```bash
./client -cmd grep_logs "Received /merge"
```

### Check re-replication events
```bash
./client -cmd grep_logs "ReReplication"
```

### Check failure detection
```bash
./client -cmd grep_logs "FAILED|SUSPECT"
```

### Verify replica operations
```bash
./client -cmd grep_logs "RECEIVED.*write.*COMPLETED"
```

## Performance Measurement Commands (for Report)

### Measure re-replication time (for various file sizes)
```bash
for size in small_4kb medium_128kb large_512kb xlarge_1mb xxlarge_5mb; do
  echo "Testing with $size"
  ./client -cmd create testdata/${size}.bin perf_${size}.bin
  REPLICAS=$(./client -cmd ls perf_${size}.bin | grep "fa25" | head -1 | awk '{print $2}')
  echo "Killing replica at $REPLICAS"
  # Kill and measure time to re-replicate
  # Record bandwidth
done
```

### Measure rebalancing overhead
```bash
# Preload 100 files, add node, measure bandwidth
```

### Measure merge performance
```bash
# Use multiappend with varying concurrent clients (1, 2, 5, 10)
# Measure merge completion time
```
