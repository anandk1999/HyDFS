# MP3-G02 — HyDFS Cluster Testbed

This repository contains our HyDFS implementation and a set of helper scripts to deploy, run, and measure system behavior across a 10‑VM cluster used for the MP assignment.

## Quick overview
- `scripts/cluster.sh` — full cluster orchestration (setup, build, start, stop, clean, cmd, logs, rejoin, leave)
- `scripts/test*.sh` — test scenarios (test1..test5)
- `hosts.txt` — list of the cluster hostnames (one per line)

## Cluster management: `scripts/cluster.sh`
This is the recommended, all-in-one tool to operate the cluster.

Common commands (run from repo root):

- Start the cluster (VM1 is introducer):

  ./scripts/cluster.sh start

- Stop all daemons:

  ./scripts/cluster.sh stop

- Clean (stop + remove logs & storage):

  ./scripts/cluster.sh clean

- Run a client command on a specific VM (VM index = line number in `hosts.txt`):

  ./scripts/cluster.sh cmd 3 create /tmp/localfile mydfsfile

- Tail/grep node logs across the cluster:

  ./scripts/cluster.sh logs "SUSPECT|ERROR|panic|FAILED"

- Rejoin a node (useful if a node is missing):

  ./scripts/cluster.sh rejoin <VM_INDEX>

  `rejoin` kills the old process on that node, clears storage, rebuilds, and starts the daemon joined to the introducer.

## Tests (test1..test5)
Each `scripts/test*.sh` has a usage header at the top. Tests generally expect the repo and `client` binary to be present on the VM that will run the client.

Examples:

- Test 1 — create 5 files sequentially (specify VM number and business file numbers):

  # Create files using VM1 and business files 10,2,3,4,5
  ./scripts/test1_create.sh 1 10 2 3 4 5

  This will create five files in HyDFS using the specified business files.

- Test 2 — get file and verify replicas (tests file retrieval and replica placement):

  # Get a file using different reader/writer VMs
  ./scripts/test2_get_and_replicas.sh demo_business_1.txt

  This tests file retrieval and verifies replica placement on the ring.

- Test 3 — test re-replication after node failure:

  # Kill nodes 3 and 5, then verify re-replication of the file
  ./scripts/test3_rereplication.sh demo_business_1.txt 3 5

  This kills specified nodes and verifies the system properly re-replicates the data.

- Test 4 — verify append ordering and read-my-writes:

  # From VM1, append business files 5 and 10 to demo_foo.txt
  ./scripts/test4_append_ordering.sh 1 demo_foo.txt 5 10

  This verifies append ordering and read-my-writes semantics.

- Test 5 — multiappend + merge with concurrent clients:

  # Run concurrent appends from VMs 1,2,3,4 using business files 5,10,15,20
  ./scripts/test5_multiappend_merge.sh 1 demo_foo.txt 1 5 2 10 3 15 4 20

  This tests concurrent appends from multiple clients followed by a merge operation.