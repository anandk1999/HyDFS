#!/bin/bash

# ==============================================================================
# HyDFS Cluster Management Script for CS425 VMs
#
# This script orchestrates the deployment, execution, and testing of the
# HyDFS service across all 10 remote VMs listed in hosts.txt.
#
# USAGE:
#   ./cluster.sh <command> [args...]
#
# COMMANDS:
#   setup              Clones the repo and builds the binary on all VMs (run once).
#   pull               Runs 'git pull' on all VMs to update the code.
#   build              Runs 'go build' on all VMs.
#   start              Starts the HyDFS daemon on all 10 VMs.
#   stop               Stops all HyDFS daemons on all VMs.
#   clean              Stops daemons and removes all logs and storage data on all VMs.
#   cmd <vm> <...>     Executes a client command on a specific VM.
#                      - <vm>: The VM index (1-10 as per hosts.txt).
#                      - <...>: The command and arguments (e.g., create, get, ls).
#                      e.g., ./cluster.sh cmd 3 create local.txt dfs_file.txt
#   logs <pattern>     Remotely greps logs on all VMs for the given pattern.
#                      e.g., ./cluster.sh logs "SUSPECT|FAILED"
#   leave <vm>         Makes a specific VM gracefully leave the cluster.
#                      e.g., ./cluster.sh leave 3
#   rejoin <vm>        Rejoins a specific VM to the cluster (kills old process, clears storage).
#                      e.g., ./cluster.sh rejoin 3
#
# ==============================================================================

# --- Configuration ---
# Use zsh-compatible syntax instead of BASH_SOURCE
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load REMOTE_USER from .env if it exists
if [ -f "${SCRIPT_DIR}/.env" ]; then
    source "${SCRIPT_DIR}/.env"
else
    echo "Warning: .env file not found at ${SCRIPT_DIR}/.env"
    echo "Using default REMOTE_USER=saik2"
    REMOTE_USER="saik2"
fi
PROJECT_DIR_NAME="mp3-g02" # Assumes this is the name of your repo's directory
BINARY_NAME="client"
HOSTS_FILE="${SCRIPT_DIR}/../hosts.txt"

# Ensure hosts file exists
if [ ! -f "$HOSTS_FILE" ]; then
    echo "Error: hosts.txt not found in parent directory."
    exit 1
fi

# Read hosts into array (bash-compatible)
HOSTS=()
while IFS= read -r line; do
    HOSTS+=("$line")
done < "$HOSTS_FILE"
NUM_VMS=${#HOSTS[@]}

# --- Colors for better output ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# --- Main Command Logic ---
COMMAND=$1
shift

run_on_all() {
    local cmd_to_run=$1
    echo -e "${YELLOW}Executing on all ${NUM_VMS} VMs: '${cmd_to_run}'...${NC}"
    for host in "${HOSTS[@]}"; do
        (
            echo -e "${CYAN}--> ${host}${NC}"
            ssh "${REMOTE_USER}@${host}" "${cmd_to_run}"
        ) &
    done
    wait
    echo -e "${GREEN}Done.${NC}"
}

case "$COMMAND" in
    setup)
        echo "This will clone your repo and build the binary on all VMs."
        run_on_all "
            if [ -d '${PROJECT_DIR_NAME}' ]; then
                echo 'Directory ${PROJECT_DIR_NAME} already exists. Skipping clone.';
            else
                git clone git@gitlab.engr.illinois.edu:saik2/mp2-g02.git ${PROJECT_DIR_NAME};
            fi &&
            cd ${PROJECT_DIR_NAME} &&
            echo 'Building binary...' &&
            go build -o ${BINARY_NAME} .
        "
        ;;
    pull)
        run_on_all "cd ${PROJECT_DIR_NAME} && git pull"
        ;;
    build)
        run_on_all "cd ${PROJECT_DIR_NAME} && go build -o ${BINARY_NAME} ."
        ;;
    start)
        echo -e "${YELLOW}Starting HyDFS cluster on all ${NUM_VMS} VMs...${NC}"

        # Get the IP of the introducer (VM1)
        # bash arrays are 0-indexed
        INTRODUCER_HOST=${HOSTS[0]}
        INTRODUCER_IP=$(ssh "${REMOTE_USER}@${INTRODUCER_HOST}" "hostname -i" | tr -d '[:space:]')
        INTRODUCER_ADDR="${INTRODUCER_IP}:8080"
        echo "Introducer is ${INTRODUCER_HOST} at ${INTRODUCER_ADDR}"

        # Start introducer (VM1)
        echo "Starting introducer daemon on ${INTRODUCER_HOST}..."
        ssh "${REMOTE_USER}@${INTRODUCER_HOST}" "
            cd ${PROJECT_DIR_NAME} &&
            go build -o ${BINARY_NAME} . &&
            nohup ./${BINARY_NAME} -port 8080 -control-port 18080 -is-introducer > node.log 2>&1 &
        "

        sleep 2 # Give it a moment to come up

        # Start other nodes (bash for loop with range)
        for ((i=1; i<NUM_VMS; i++)); do
            host=${HOSTS[$i]}
            port=$((8080 + i))
            cport=$((18080))
            echo "Starting daemon on ${host} (ports ${port}/${cport}), joining introducer..."
            ssh "${REMOTE_USER}@${host}" "
                cd ${PROJECT_DIR_NAME} &&
                go build -o ${BINARY_NAME} . &&
                nohup ./${BINARY_NAME} -port ${port} -control-port ${cport} -introducer ${INTRODUCER_ADDR} > node.log 2>&1 &
            " &
        done
        wait
        echo -e "${GREEN}All daemons started.${NC}"
        ;;
    stop)
    run_on_all "pkill -f '${PROJECT_DIR_NAME}/${BINARY_NAME}' || echo 'No process found to kill.'"
        ;;
    clean)
        echo -e "${RED}WARNING: This will stop all nodes and delete ALL logs and storage data.${NC}"
        if read -k 1 "REPLY?Are you sure? (y/N) "; then
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                echo "Stopping all daemons..."
                run_on_all "pkill -9 -f '${PROJECT_DIR_NAME}/${BINARY_NAME}' || true"
                
                echo "Waiting for processes to terminate..."
                sleep 2

                echo "Deleting storage and logs on all VMs..."
                run_on_all "cd \${HOME}/${PROJECT_DIR_NAME} && rm -rfv hydfs_storage logs node.log"
            else
                echo "Clean cancelled."
            fi
        else
             echo "\nClean cancelled."
        fi
        ;;
    cmd)
        if [ -z "$1" ]; then
            echo -e "${RED}Error: 'cmd' requires a VM index (1-10).${NC}"
            exit 1
        fi
        VM_INDEX=$1
        shift

        if [ "${VM_INDEX}" -lt 1 ] || [ "${VM_INDEX}" -gt "${NUM_VMS}" ]; then
            echo -e "${RED}Error: Invalid VM index. Must be between 1 and ${NUM_VMS}.${NC}"
            exit 1
        fi
        
        # bash arrays are 0-indexed, so subtract 1 from user input
        TARGET_HOST=${HOSTS[$((VM_INDEX - 1))]}
        CONTROL_PORT=$((18080))
        
        # Remaining args are the command and its arguments
        remote_args=("$@")
        
        echo -e "${YELLOW}Executing on ${TARGET_HOST}: ./${BINARY_NAME} -control-port ${CONTROL_PORT} -cmd ${remote_args[*]}${NC}"
        ssh "${REMOTE_USER}@${TARGET_HOST}" "
            cd ${PROJECT_DIR_NAME} &&
            go build -o ${BINARY_NAME} . &&
            ./${BINARY_NAME} -control-port ${CONTROL_PORT} -cmd ${remote_args[*]}
        "
        ;;
    logs)
        if [ -z "$1" ]; then
            echo -e "${RED}Error: 'logs' requires a pattern to grep for.${NC}"
            exit 1
        fi
        PATTERN=$1
        run_on_all "cd ${PROJECT_DIR_NAME} && grep -H \"${PATTERN}\" node.log"
        ;;
    leave)
        if [ -z "$1" ]; then
            echo -e "${RED}Error: 'leave' requires a VM index (1-10).${NC}"
            exit 1
        fi
        VM_INDEX=$1

        if [ "${VM_INDEX}" -lt 1 ] || [ "${VM_INDEX}" -gt "${NUM_VMS}" ]; then
            echo -e "${RED}Error: Invalid VM index. Must be between 1 and ${NUM_VMS}.${NC}"
            exit 1
        fi

    TARGET_HOST=$(sed -n "${VM_INDEX}p" "${HOSTS_FILE}")
        echo -e "${YELLOW}Making node ${VM_INDEX} (${TARGET_HOST}) leave the cluster...${NC}"

        # Execute leave command via client
        ssh "${REMOTE_USER}@${TARGET_HOST}" "
            cd ${PROJECT_DIR_NAME} &&
            ./${BINARY_NAME} -cmd leave
        "
        
        echo -e "${GREEN}Node ${VM_INDEX} has left the cluster.${NC}"
        ;;
    rejoin)
        if [ -z "$1" ]; then
            echo -e "${RED}Error: 'rejoin' requires a VM index (1-10).${NC}"
            exit 1
        fi
        VM_INDEX=$1

        if [ "${VM_INDEX}" -lt 1 ] || [ "${VM_INDEX}" -gt "${NUM_VMS}" ]; then
            echo -e "${RED}Error: Invalid VM index. Must be between 1 and ${NUM_VMS}.${NC}"
            exit 1
        fi

        # Get introducer address
        INTRODUCER_HOST=${HOSTS[1]}
        INTRODUCER_IP=$(ssh "${REMOTE_USER}@${INTRODUCER_HOST}" "hostname -i" | tr -d '[:space:]')
        INTRODUCER_ADDR="${INTRODUCER_IP}:8080"

    TARGET_HOST=$(sed -n "${VM_INDEX}p" "${HOSTS_FILE}")
        port=$((8080 + VM_INDEX - 1))
        cport=$((18080))

        echo -e "${YELLOW}Rejoining node ${VM_INDEX} (${TARGET_HOST}) to cluster...${NC}"
        echo "Introducer: ${INTRODUCER_ADDR}"
        echo "Node will use ports ${port}/${cport}"

        ssh "${REMOTE_USER}@${TARGET_HOST}" "
            set -e
            cd ${PROJECT_DIR_NAME}

            # Find and kill the specific process listening on the control port
            PID_TO_KILL=\$(lsof -t -i:${cport} -sTCP:LISTEN)
            if [ -n \"\$PID_TO_KILL\" ]; then
                echo 'Found old daemon process \$PID_TO_KILL, killing it...'
                kill -9 \$PID_TO_KILL || true
                sleep 1
            else
                echo 'No old daemon process found running.'
            fi

            echo 'Cleaning up old storage...'
            rm -rf hydfs_storage

            echo 'Rebuilding binary...'
            go build -o ${BINARY_NAME} .

            echo 'Starting new daemon...'
            nohup ./${BINARY_NAME} -port ${port} -control-port ${cport} -introducer ${INTRODUCER_ADDR} > node.log 2>&1 &
        "
        
        echo -e "${GREEN}Node ${VM_INDEX} rejoin command sent. Check logs with: ./cluster.sh logs 'Joined the group'${NC}"
        ;;
    *)
        echo "Usage: $0 {setup|pull|build|start|stop|clean|cmd|logs|leave|rejoin} [args...]"
        exit 1
        ;;
esac

### Step 3: Deployment and Testing Workflow

# Follow these steps from your local machine to manage the 10-VM cluster.

# 1.  **Make the Script Executable:**
#     ```bash
#     chmod +x scripts/cluster.sh
#     ```

# 2.  **Initial Setup (Run Once):** This clones your repository and builds the application on all 10 VMs.
#     ```bash
#     ./scripts/cluster.sh setup
#     ```

# 3.  **Update and Rebuild:** After you make local code changes and push them to GitLab, run these commands to update all VMs.
#     ```bash
#     # Pull the latest code from your GitLab repo on all VMs
#     ./scripts/cluster.sh pull

#     # Rebuild the binary on all VMs
#     ./scripts/cluster.sh build
#     ```

# 4.  **Start the Cluster:** This will stop any old processes and start a fresh 10-node cluster, with VM 1 as the introducer.
#     ```bash
#     # Stop any running nodes first to ensure a clean start
#     ./scripts/cluster.sh stop

#     # Start the 10-node cluster in the background
#     ./scripts/cluster.sh start
#     ```
#     Your HyDFS cluster is now running!

# 5.  **Test the Cluster:** Now you can interact with it using the `cmd` subcommand.

#     * **Check the membership list from VM 2:**
#         ```bash
#         ./scripts/cluster.sh cmd 2 list_mem_ids
#         ```

#     * **Create local test files:**
#         ```bash
#         echo "This data comes from my local machine." > data1.txt
#         echo "Appending this second piece of data." > data2.txt
#         ```

#     * **Create a file on HyDFS using VM 3 as the client:**
#         *(The script automatically detects `data1.txt` is a local file and SCPs it to a temporary location on VM 3 before running the command).*
#         ```bash
#         ./scripts/cluster.sh cmd 3 create data1.txt my_first_dfs_file
#         ```

#     * **Check which nodes are storing the file (from any VM, e.g., VM 8):**
#         ```bash
#         ./scripts/cluster.sh cmd 8 ls my_first_dfs_file
#         ```

#     * **Append to the file from a different client (e.g., VM 10):**
#         ```bash
#         ./scripts/cluster.sh cmd 10 append data2.txt my_first_dfs_file
#         ```

#     * **Monitor logs for interesting events (like failures or internal writes):**
#         ```bash
#         ./scripts/cluster.sh logs "SUSPECT|FAILED|HyDFS-Replica"
#         ```

#     * **Download the final file from a third client (e.g., VM 5):**
#         ```bash
#         ./scripts/cluster.sh cmd 5 get my_first_dfs_file downloaded.txt
        
