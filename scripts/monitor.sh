#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"/..

if [ ! -d "logs" ]; then
    echo -e "${RED}Logs directory not found. Has the system been started?${NC}"
    exit 1
fi

show_status() {
    echo -e "${BLUE}=== System Status ===${NC}"

    echo -e "${YELLOW}Running Components:${NC}"
    for pid_file in logs/*.pid; do
        if [ -f "$pid_file" ]; then
            component=$(basename "$pid_file" .pid)
            pid=$(cat "$pid_file")
            if ps -p $pid > /dev/null; then
                runtime=$(ps -o etime= -p $pid)
                echo -e "${GREEN}✓ ${component} is running (PID: $pid, Runtime: $runtime)${NC}"
            else
                echo -e "${RED}✗ ${component} is not running${NC}"
            fi
        fi
    done

    echo -e "\n${YELLOW}RabbitMQ Queues:${NC}"
    if command -v rabbitmqctl &> /dev/null; then
        rabbitmqctl list_queues name messages_ready messages_unacknowledged | grep -v "Timeout"
    else
        echo -e "${CYAN}Install rabbitmqctl to see queue status${NC}"
    fi
}

follow_logs() {
    local component=$1
    local log_file="logs/${component}.log"

    if [ -f "$log_file" ]; then
        echo -e "${GREEN}Following logs for ${component}. Press Ctrl+C to stop.${NC}"
        tail -f "$log_file"
    else
        echo -e "${RED}Log file for ${component} not found.${NC}"
        echo -e "Available components:"
        for log_file in logs/*.log; do
            if [ -f "$log_file" ]; then
                echo -e "  - $(basename "$log_file" .log)"
            fi
        done
    fi
}

# Main script logic
if [ "$1" == "status" ] || [ -z "$1" ]; then
    show_status
elif [ "$1" == "logs" ]; then
    if [ -z "$2" ]; then
        echo -e "${RED}Please specify a component to view logs for.${NC}"
        echo -e "Usage: $0 logs [component]"
        echo -e "Available components:"
        for log_file in logs/*.log; do
            if [ -f "$log_file" ]; then
                echo -e "  - $(basename "$log_file" .log)"
            fi
        done
    else
        follow_logs "$2"
    fi
else
    echo -e "${RED}Unknown command: $1${NC}"
    echo -e "Usage: $0 [status|logs component]"
fi
