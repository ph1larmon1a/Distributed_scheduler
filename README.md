## Distributed Scheduler

A distributed job scheduling system that handles task submissions from clients, assigns them to available worker nodes, and monitors worker health. The system support round-robin scheduling, task prioritization, and fault-tolerant task reassignment if a worker becomes unavailable. Heartbeat messages are used to detect failures, and the scheduler should reassign tasks from failed nodes to active ones. The system also track task statuses and enable performance visualization.

To run the system you need to:

1. Run RabbitMQ server
2. Run Scheduler server
3. Run Logger server
4. Run Workers
5. Generate tasks

To do this you can use the following commands:

Run RabbitMQ cluster:

```
cd rabbitmq
docker-compose up -d
```

Run scheduler system:

```
./scripts/system_run.sh
```
