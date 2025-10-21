#!/bin/zsh

# Runs a git (or any) command inside the mp3-g02 repo on each host.
# Example: ./git_command.sh "git status -sb"
# Load remote username
SCRIPT_PATH=${0:A}
SCRIPT_DIR="$(cd "${SCRIPT_PATH:h}" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"

# Ask user for the command to run on all hosts if not passed as an argument
USER_CMD="$1"
if [[ -z "$USER_CMD" ]]; then
  printf "Enter the command to run on all hosts: "
  read USER_CMD
fi

if [[ -z "$USER_CMD" ]]; then
  echo "No command entered. Exiting."
  exit 1
fi

HOSTS_FILE="../hosts.txt"
for HOST in $(cat $HOSTS_FILE); do
  (
    echo ">>> Running $USER_CMD in repo on $HOST"
  ssh "$REMOTE_USER@$HOST" "cd mp3-g02 && $USER_CMD"
  ) &
done
wait