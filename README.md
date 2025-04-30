## Distributed Scheduler

A distributed job scheduling system that handles task submissions from clients, assigns them to available worker nodes, and monitors worker health. The system support round-robin scheduling, task prioritization, and fault-tolerant task reassignment if a worker becomes unavailable. Heartbeat messages are used to detect failures, and the scheduler should reassign tasks from failed nodes to active ones. The system also track task statuses and enable performance visualization.

## How To Run

### Install Dependencies

Firstly, you need to install libraries for Python:

```bash
pip install -r requirements.txt
```

### Run System

To run the system you need to:

1. Run RabbitMQ server
2. Run Scheduler server
3. Run Logger server
4. Run Workers
5. Generate tasks

To do this you can use the following commands:

Copy environment variables:

```bash
cp .env.example .env
```

Run RabbitMQ cluster with Grafana:

```bash
cd rabbitmq
docker-compose up -d
```

Or you can run only RabbitMQ by hands.

Run scheduler system:

```bash
./scripts/system_run.sh
```

After running this script, the system will generate and handle jobs automatically.
You can monitor the system status by checking the logs of the logger service and checking the RabbitMQ server using the RabbitMQ management UI (http://localhost:15672) or Grafana (http://localhost:3000).
