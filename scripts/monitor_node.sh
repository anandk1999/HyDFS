#!/bin/bash
# Live monitoring script - shows SUSPECT events and allows CLI commands
# Usage: ./scripts/monitor_node.sh [host] [port]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env"
REMOTE_USER="${REMOTE_USER:-saik2}"
HOST=${1:-$(hostname)}
PORT=${2:-8080}

STATE_FILE=$(mktemp -t mp2-monitor-state.XXXXXX)
EVENT_FILE=$(mktemp -t mp2-monitor-events.XXXXXX)

# Local monitoring
echo "Monitoring local node on port $PORT"
echo "Commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, grep_logs, show_status, show_events, quit"
echo "You can add arguments after a command, e.g.: switch -mode gossip -interval 500ms"
echo "Membership events (SUSPECT/FAILED/CLEARED/joins/leaves) will appear automatically below:"
echo "Note: Exact duplicate lines (including timestamp) are filtered out"
echo "----------------------------------------"

# Start background process to show suspects from log
if [ -f "node.log" ]; then
    # Show only the latest SUSPECT, FAILED, CLEARED events and member joins/leaves
    # Use bash subshell for better process control on macOS
    (
        tail -F node.log 2>/dev/null | \
            grep --line-buffered -E "(SUSPECT|FAILED|CLEARED|joined|left|New member|cleared suspicion)" | \
            awk -v state_file="$STATE_FILE" -v event_file="$EVENT_FILE" '
            function flush_state(   node) {
                close(state_file)
                print "" > state_file
                close(state_file)
                for (node in current_state) {
                    printf "%s %s\n", node, current_state[node] >> state_file
                }
                close(state_file)
            }

            function emit(ts, status, node, raw,   color_start, color_end, summary) {
                color_start=""
                color_end="\033[0m"
                if (status == "FAILED") color_start="\033[31m"
                else if (status == "SUSPECT") color_start="\033[33m"
                else if (status == "CLEARED") color_start="\033[32m"
                else if (status == "JOINED" || status == "NEW_MEMBER") color_start="\033[36m"
                else if (status == "LEFT") color_start="\033[35m"
                else color_end=""

                summary = ts " [" status "]"
                if (node != "") {
                    summary = summary " " node
                }

                if (color_end != "") {
                    printf "%s%s %s%s\n", color_start, summary, raw, color_end
                } else {
                    printf "%s %s\n", summary, raw
                }
                fflush()

                printf "%s %s\n", summary, raw >> event_file
                close(event_file)
            }

            {
                ts = $1 " " $2
                line = $0
                status = ""
                node = ""

                if (match(line, /SUSPECT: ([^ ]+)/, arr)) {
                    status = "SUSPECT"; node = arr[1]
                } else if (match(line, /FAILED: ([^ ]+)/, arr)) {
                    status = "FAILED"; node = arr[1]
                } else if (match(line, /CLEARED: ([^ ]+)/, arr)) {
                    status = "CLEARED"; node = arr[1]
                } else if (match(line, /New member joined: ([^ ]+)/, arr)) {
                    status = "JOINED"; node = arr[1]
                } else if (match(line, /left the group: ([^ ]+)/, arr)) {
                    status = "LEFT"; node = arr[1]
                } else if (match(line, /Member ([^ ]+) cleared suspicion/, arr)) {
                    status = "CLEARED"; node = arr[1]
                } else {
                    next
                }

                key = (node != "" ? node : line)

                message = substr(line, length($1 " " $2 " ") + 1)

                if (current_state[key] != status || last_line[key] != line) {
                    current_state[key] = status
                    last_line[key] = line
                    emit(ts, status, node, message)
                    flush_state()
                }
            }'
    ) &
    TAIL_PID=$!
fi

# Cleanup function - kills all background processes
cleanup() {
    echo ""
    echo "Cleaning up background processes..."
    
    # Kill the tail process and its children
    if [ -n "$TAIL_PID" ]; then
        # Kill the entire process group to ensure all pipeline processes are terminated
        kill -TERM -$TAIL_PID 2>/dev/null || kill $TAIL_PID 2>/dev/null
        
        # Wait a moment for graceful termination
        sleep 0.2
        
        # Force kill if still running
        kill -KILL -$TAIL_PID 2>/dev/null || kill -KILL $TAIL_PID 2>/dev/null
    fi
    
    # Kill any remaining background jobs from this shell
    jobs -p | xargs -r kill -TERM 2>/dev/null
    sleep 0.1
    jobs -p | xargs -r kill -KILL 2>/dev/null
    
    rm -f "$STATE_FILE" "$EVENT_FILE"
    echo "Cleanup complete."
    exit 0
}

# Set up signal traps for cleanup
trap cleanup INT TERM EXIT QUIT

# Interactive CLI loop
while true; do
    echo ""
    read -r -p "mp2-node> " line

    # Enable extglob for trimming if available
    shopt -s extglob 2>/dev/null || true
    line="${line##+([[:space:]])}"; line="${line%%+([[:space:]])}" 2>/dev/null || true
    shopt -u extglob 2>/dev/null || true

    if [ -z "$line" ]; then
        continue
    fi

    set -f
    if ! eval "set -- $line"; then
        echo "Unable to parse command. Check your quoting."
        set +f
        continue
    fi
    set +f

    cmd="$1"
    shift

    case "$cmd" in
        ""|"help")
            echo "Available commands: list_mem, list_self, display_suspects, display_protocol, join, leave, switch, grep_logs, show_status, show_events, quit"
            echo "Examples:"
            echo "  switch -mode gossip -interval 500ms"
            echo "  switch -mode ping -suspicion -interval 200ms"
            echo "  join 172.22.156.123:8080"
            ;;
        "grep_logs")
            grep_cmd=""
            file_type=""
            hosts_value=""
            hosts_file_value=""
            port_value=""
            timeout_value=""
            declare -a extra_args=()
            invalid=0

            while [ $# -gt 0 ]; do
                case "$1" in
                    -arg1)
                        shift
                        if [ $# -eq 0 ]; then
                            echo "grep_logs: -arg1 requires a value"
                            invalid=1
                            break
                        fi
                        grep_cmd="$1"
                        shift
                        ;;
                    -arg2)
                        shift
                        if [ $# -eq 0 ]; then
                            echo "grep_logs: -arg2 requires a value"
                            invalid=1
                            break
                        fi
                        file_type="$1"
                        shift
                        ;;
                    -hosts)
                        shift
                        if [ $# -eq 0 ]; then
                            echo "grep_logs: -hosts requires a value"
                            invalid=1
                            break
                        fi
                        hosts_value="$1"
                        shift
                        ;;
                    -hosts-file)
                        shift
                        if [ $# -eq 0 ]; then
                            echo "grep_logs: -hosts-file requires a value"
                            invalid=1
                            break
                        fi
                        hosts_file_value="$1"
                        shift
                        ;;
                    -port)
                        shift
                        if [ $# -eq 0 ]; then
                            echo "grep_logs: -port requires a value"
                            invalid=1
                            break
                        fi
                        port_value="$1"
                        shift
                        ;;
                    -timeout)
                        shift
                        if [ $# -eq 0 ]; then
                            echo "grep_logs: -timeout requires a value"
                            invalid=1
                            break
                        fi
                        timeout_value="$1"
                        shift
                        ;;
                    *)
                        extra_args+=("$1")
                        shift
                        ;;
                esac
            done

            if [ $invalid -eq 1 ]; then
                continue
            fi

            if [ -z "$grep_cmd" ]; then
                echo "Usage: grep_logs -arg1 \"grep ...\" [-arg2 node]"
                continue
            fi

            if [ -z "$file_type" ]; then
                file_type="node"
            fi

            if [ -x "./logquery" ]; then
                logquery_cmd=("./logquery")
            elif [ -x "./bin/logquery" ]; then
                logquery_cmd=("./bin/logquery")
            else
                logquery_cmd=(go run ./cmd/logquery)
            fi

            cmd_args=("${logquery_cmd[@]}" -grep "$grep_cmd")

            if [ -n "$file_type" ]; then
                cmd_args+=(-file "$file_type")
            fi

            if [ -n "$hosts_value" ]; then
                cmd_args+=(-hosts "$hosts_value")
            fi

            hosts_file_to_use="$hosts_file_value"
            if [ -z "$hosts_file_to_use" ] && [ -n "$MP2_LOG_HOSTS_FILE" ]; then
                hosts_file_to_use="$MP2_LOG_HOSTS_FILE"
            fi
            if [ -n "$hosts_file_to_use" ]; then
                cmd_args+=(-hosts-file "$hosts_file_to_use")
            fi

            if [ -n "$port_value" ]; then
                cmd_args+=(-port "$port_value")
            elif [ -n "$MP2_LOG_PORT" ]; then
                cmd_args+=(-port "$MP2_LOG_PORT")
            fi

            if [ -n "$timeout_value" ]; then
                cmd_args+=(-timeout "$timeout_value")
            fi

            if [ ${#extra_args[@]} -gt 0 ]; then
                cmd_args+=("${extra_args[@]}")
            fi

            echo "Running log query: ${cmd_args[*]}"
            "${cmd_args[@]}"
            ;;
        "quit"|"exit"|"q")
            echo "Goodbye!"
            cleanup
            ;;
        "show_status")
            if [ -s "$STATE_FILE" ]; then
                echo "Current per-node event state:"
                if command -v column >/dev/null 2>&1; then
                    column -t "$STATE_FILE"
                else
                    cat "$STATE_FILE"
                fi
            else
                echo "No events recorded yet."
            fi
            ;;
        "show_events"|"events")
            if [ -s "$EVENT_FILE" ]; then
                echo "Recent event transitions:"
                tail -n 20 "$EVENT_FILE"
            else
                echo "No events recorded yet."
            fi
            ;;
        *)
            # Forward the command to the actual application
            if [ -n "$cmd" ]; then
                ./mp2-node -cmd "$cmd" "$@" || echo "Error: Could not connect to node on port $PORT"
            fi
            ;;
    esac
done
