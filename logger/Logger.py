# ==================================
#
# This file was taken from 3rd lab of [S25] DNP course
#
# ==================================


import pika
from datetime import datetime
import json
from dotenv import load_dotenv
from pathlib import Path
import os

root_dir = Path(__file__).resolve().parent.parent
load_dotenv(root_dir / '.env')

RMQ_HOST = os.getenv('RMQ_HOST', 'localhost')
RMQ_USER = os.getenv('RMQ_USER', 'guest')
RMQ_PASS = os.getenv('RMQ_PASS', 'guest')
EXCHANGE_NAME = os.getenv('EXCHANGE_NAME', 'numbers')
LOG_FILE = os.getenv('LOG_FILE', 'rabbitmq_messages.log')

LOGGER_QUEUE = os.getenv('LOGGER_QUEUE', 'system_logs_queue')

def setup_logging():
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    with open(LOG_FILE, 'w') as f:
        f.write(f"\n\n=== Log started at {datetime.now().isoformat()} ===\n")

    print(f"Logging to file: {os.path.abspath(LOG_FILE)}")

def log_event_callback(ch, method, properties, body):
    try:
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            log_entry = f"[{datetime.now().isoformat()}] RAW MESSAGE: {body}\n"
            with open(LOG_FILE, 'a') as f:
                f.write(log_entry)
            print(log_entry.strip())
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        event_type = data.get('event_type', 'UNKNOWN')
        component = data.get('component', 'system')
        worker_id = data.get('worker_id', 'unknown')
        details = data.get('details', {})
        timestamp = data.get('timestamp', datetime.now().isoformat())

        if event_type == 'JOB_SUBMITTED':
            job_id = details.get('job_id', 'unknown')
            execution_time = details.get('execution_time', 'unknown')
            priority = details.get('priority', 'unknown')
            log_entry = f"[{timestamp}] {component}: Job {job_id} submitted (execution_time={execution_time}s, priority={priority})\n"

        elif event_type == 'TASK_SCHEDULED':
            task_id = details.get('task_id', 'unknown')
            execution_time = details.get('execution_time', 'unknown')
            priority = details.get('priority', 'unknown')
            log_entry = f"[{timestamp}] {component}: Job {task_id} scheduled (execution_time={execution_time}s, priority={priority})\n"

        elif event_type == 'TASK_STARTED':
            task_id = details.get('task_id', 'unknown')
            wait_time = details.get('wait_time', 'unknown')
            log_entry = f"[{timestamp}] {component} (Worker {worker_id}): Started task {task_id} (wait_time={wait_time}s)\n"

        elif event_type == 'TASK_COMPLETED':
            task_id = details.get('task_id', 'unknown')
            duration = details.get('execution_time', 'unknown')
            log_entry = f"[{timestamp}] {component} (Worker {worker_id}): Completed task {task_id} in {duration}s\n"

        elif event_type == 'TASK_FAILED':
            task_id = details.get('task_id', 'unknown')
            reason = details.get('reason', 'unknown')
            log_entry = f"[{timestamp}] {component} (Worker {worker_id}): Failed task {task_id} - {reason}\n"

        elif event_type == 'TASK_RETRY':
            task_id = details.get('job_id', 'unknown')
            failed_worker_id = details.get('failed_worker_id', 'unknown')
            log_entry = f"[{timestamp}] {component}: Retrying task {task_id} (failed on worker {failed_worker_id})\n"

        elif event_type == 'WORKER_STARTED':
            log_entry = f"[{timestamp}] Worker {worker_id}: Started\n"

        elif event_type == 'WORKER_STOPPED':
            reason = details.get('reason', 'unknown')
            log_entry = f"[{timestamp}] Worker {worker_id}: Stopped - {reason}\n"

        elif event_type == 'SCHEDULER_STARTED':
            port = details.get('port', 'unknown')
            log_entry = f"[{timestamp}] Scheduler: Started on port {port}\n"

        elif event_type == 'SCHEDULER_STOPPED':
            reason = details.get('reason', 'unknown')
            log_entry = f"[{timestamp}] Scheduler: Stopped - {reason}\n"

        elif event_type == 'WORKER_STATS':
            active_workers = details.get('active_workers', 0)
            worker_ids = details.get('worker_ids', [])
            log_entry = f"[{timestamp}] Scheduler: Active workers: {active_workers} - {', '.join(worker_ids) if worker_ids else 'none'}\n"

        else:
            log_entry = f"[{timestamp}] {component}: {event_type} - {json.dumps(details)}\n"

        with open(LOG_FILE, 'a') as f:
            f.write(log_entry)

        print(log_entry.strip())

    except Exception as e:
        error_msg = f"[{datetime.now().isoformat()}] ERROR processing log message: {e}\n"
        with open(LOG_FILE, 'a') as f:
            f.write(error_msg)
            f.write(f"Exception details: {traceback.format_exc()}\n")
            f.write(f"Message body: {body}\n")
        print(error_msg.strip())

    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    setup_logging()

    print(f"Connecting to RabbitMQ at {RMQ_HOST}...")

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RMQ_HOST,
            credentials=pika.PlainCredentials(RMQ_USER, RMQ_PASS),
            heartbeat=600
        ))
        channel = connection.channel()

        channel.queue_declare(queue=LOGGER_QUEUE, durable=True)

        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            queue=LOGGER_QUEUE,
            on_message_callback=log_event_callback
        )

        print(f" [*] Logger started, waiting for messages on '{LOGGER_QUEUE}'. To exit press CTRL+C")

        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error connecting to RabbitMQ: {e}")
        sys.exit(1)

    except KeyboardInterrupt:
        if 'connection' in locals() and connection:
            connection.close()
        print(" [*] Logger stopped")

if __name__ == '__main__':
    main()
