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

  Notes:
  - `cmd` will run the `client` binary on the chosen VM with `-control-port 18080 -cmd ...`.
  - `cmd` builds the binary remotely before running the command.

- Tail/grep node logs across the cluster:

  ./scripts/cluster.sh logs "SUSPECT|ERROR|panic|FAILED"

- Rejoin a node (useful if a node is missing):

  ./scripts/cluster.sh rejoin <VM_INDEX>

  `rejoin` kills the old process on that node, clears storage, rebuilds, and starts the daemon joined to the introducer.

## Tests (test1..test5)
Each `scripts/test*.sh` has a usage header at the top. Tests generally expect the repo and `client` binary to be present on the VM that will run the client.

Examples:

- Test 1 — create 5 files sequentially (writer VM is argument; default `localhost`):

  ./scripts/test1_create.sh pnj2@fa25-cs425-0203.cs.illinois.edu

  Or run on a VM directly:

  ./test1_create.sh localhost

- Test 5 — multiappend + merge (example usage is in the script header):

  ./scripts/test5_multiappend_merge.sh pnj2@fa25-cs425-0203.cs.illinois.edu demo_foo.txt \
    pnj2@fa25-cs425-0203.cs.illinois.edu /home/pnj2/mp3-g02/business/business_1.txt \
    pnj2@fa25-cs425-0204.cs.illinois.edu /home/pnj2/mp3-g02/business/business_2.txt

Notes:
- When running tests from your local machine, `test*.sh` will SSH to the specified writer/initiator. Make sure input local files exist on the remote writer VM or `scp` them there first.
- Many tests assume the `business/` directory is present on each VM (it is copied by `deploy_all.sh`).