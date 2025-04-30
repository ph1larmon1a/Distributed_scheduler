#!/bin/bash

NUM_WORKERS=${1:-3}  # Number of worker processes to start
JOB_INTERVAL=${2:-5} # Seconds between submitting jobs

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Directory where this script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"/..

# Ensure .env file exists (create with defaults if it doesn't)
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating default .env file...${NC}"
    cat > .env << EOF
RMQ_HOST=localhost
RMQ_USER=guest
RMQ_PASS=guest
LOGGER_QUEUE=system_logs_queue
HEARTBEAT_QUEUE=heartbeat_queue
TASK_QUEUE=task_queue
TASK_RESULT_QUEUE=task_results
WORKER_REGISTRY_QUEUE=worker_registry
SCHEDULER_PORT=50051
WORKER_TIMEOUT=120
HEARTBEAT_INTERVAL=30
HEARTBEAT_CHECK_INTERVAL=60
FAILURE_CHANCE=10
EOF
fi

mkdir -p logs

run_component() {
    local component=$1
    local name=$2
    local log_file="logs/${name}.log"

    echo -e "${BLUE}Starting ${name}...${NC}"
    python3 $component > "$log_file" 2>&1 &
    echo $! > "logs/${name}.pid"
    echo -e "${GREEN}${name} started (PID: $(cat logs/${name}.pid)). Logs: ${log_file}${NC}"
}

stop_all() {
    echo -e "${YELLOW}Stopping all components...${NC}"
    for pid_file in logs/*.pid; do
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            component=$(basename "$pid_file" .pid)
            echo -e "${BLUE}Stopping $component (PID: $pid)...${NC}"
            kill $pid 2>/dev/null || true
            rm "$pid_file"
        fi
    done
    echo -e "${GREEN}All components stopped.${NC}"
}

trap stop_all EXIT

run_component "logger/Logger.py" "logger"
sleep 2

run_component "scheduler/Scheduler.py" "scheduler"
sleep 5

for i in $(seq 1 $NUM_WORKERS); do
    run_component "worker/Worker.py" "worker$i"
    sleep 1
done

run_component "client/run_jobs.py $JOB_INTERVAL" "job_generator"

echo -e "${GREEN}System is running!${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop all components.${NC}"

while true; do
    sleep 1
done
