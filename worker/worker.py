import pika
import sys
import uuid
import os
import json
from dotenv import load_dotenv
from datetime import datetime
import time
import threading
from pathlib import Path
import random

root_dir = Path(__file__).resolve().parent.parent
load_dotenv(root_dir / '.env')

RMQ_HOST = os.getenv('RMQ_HOST', 'localhost')
RMQ_USER = os.getenv('RMQ_USER', 'guest')
RMQ_PASS = os.getenv('RMQ_PASS', 'guest')
EXCHANGE_NAME = os.getenv('EXCHANGE_NAME', 'numbers')

LOGGER_QUEUE = os.getenv('LOGGER_QUEUE', 'system_logs_queue')
HEARTBEAT_QUEUE = os.getenv('HEARTBEAT_QUEUE', 'heartbeat_queue')
TASK_QUEUE = os.getenv('TASK_QUEUE', 'task_queue')
TASK_RESULT_QUEUE = os.getenv('TASK_RESULT_QUEUE', 'task_results')
WORKER_REGISTRY_QUEUE = os.getenv('WORKER_REGISTRY_QUEUE', 'worker_registry')

WORKER_ID = str(uuid.uuid4())
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', '30'))

FAILURE_CHANCE = int(os.getenv('FAILURE_CHANCE', '0'))

def create_rabbitmq_connection():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RMQ_HOST,
            credentials=pika.PlainCredentials(RMQ_USER, RMQ_PASS),
            heartbeat=600
        ))
        channel = connection.channel()

        channel.queue_declare(queue=LOGGER_QUEUE, durable=True)
        channel.queue_declare(queue=HEARTBEAT_QUEUE, durable=True)
        channel.queue_declare(queue=TASK_QUEUE, durable=True)
        channel.queue_declare(queue=TASK_RESULT_QUEUE, durable=True)
        channel.queue_declare(queue=WORKER_REGISTRY_QUEUE, durable=True)

        return connection, channel
    except Exception as e:
        print(f"Failed to connect to RabbitMQ: {e}")
        sys.exit(1)

def log_event(channel, event_type, details):
    try:
        channel.basic_publish(
            exchange='',
            routing_key=LOGGER_QUEUE,
            body=json.dumps({
                'event_type': event_type,
                'component': 'worker',
                'worker_id': WORKER_ID,
                'details': details,
                'timestamp': datetime.now().isoformat()
            }),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
    except Exception as e:
        print(f"Failed to send log: {e}")

def register_with_scheduler(channel):
    try:
        channel.basic_publish(
            exchange='',
            routing_key=WORKER_REGISTRY_QUEUE,
            body=json.dumps({
                'worker_id': WORKER_ID,
                'status': 'available',
                'action': 'register',
                'timestamp': datetime.now().isoformat()
            }),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        print(f"Worker {WORKER_ID} registered with scheduler")
        return True
    except Exception as e:
        print(f"Failed to register with scheduler: {e}")
        return False

def send_heartbeat(channel):
    try:
        channel.basic_publish(
            exchange='',
            routing_key=HEARTBEAT_QUEUE,
            body=json.dumps({
                'worker_id': WORKER_ID,
                'status': 'active',
                'timestamp': datetime.now().isoformat()
            })
        )
        print(f"Heartbeat sent")
        return True
    except Exception as e:
        print(f"Failed to send heartbeat: {e}")
        return False

def heartbeat_loop():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RMQ_HOST,
        credentials=pika.PlainCredentials(RMQ_USER, RMQ_PASS)
    ))
    channel = connection.channel()

    try:
        while True:
            send_heartbeat(channel)
            time.sleep(HEARTBEAT_INTERVAL)
    except Exception as e:
        print(f"Error in heartbeat loop: {e}")
    finally:
        connection.close()

def process_task(ch, method, properties, body):
    try:
        data = json.loads(body)
        task_id = data.get('task_id', 'unknown')
        wait_time = data.get('wait_time', 5)

        log_event(ch, 'JOB_STARTED', {
            'job_id': task_id,
            'wait_time': wait_time
        })

        print(f" [x] Processing job {task_id}, waiting for {wait_time}s")

        time.sleep(wait_time)

        if random.random() * 100 < FAILURE_CHANCE:
            print(f" [!] Job {task_id} failed")
            log_event(ch, 'JOB_FAILED', {
                'job_id': task_id,
                'reason': 'random failure'
            })
            ch.basic_publish(
                exchange='',
                routing_key=TASK_RESULT_QUEUE,
                body=json.dumps({
                    'job_id': task_id,
                    'worker_id': WORKER_ID,
                    'status': 'failed',
                    'timestamp': datetime.now().isoformat()
                })
            )
        else:
            print(f" [🎉] Job {task_id} completed")
            log_event(ch, 'JOB_COMPLETED', {
                'job_id': task_id,
                'duration': wait_time
            })
            ch.basic_publish(
                exchange='',
                routing_key=TASK_RESULT_QUEUE,
                body=json.dumps({
                    'task_id': task_id,
                    'worker_id': WORKER_ID,
                    'status': 'completed',
                    'timestamp': datetime.now().isoformat()
                })
            )
    except Exception as e:
        print(f"Error processing task: {e}")
        log_event(ch, 'ERROR', {
            'error': str(e)
        })
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection, channel = create_rabbitmq_connection()

    channel.basic_qos(prefetch_count=1)

    if not register_with_scheduler(channel):
        print("Failed to register with scheduler, exiting")
        connection.close()
        return

    log_event(channel, 'WORKER_STARTED', {
        'id': WORKER_ID
    })

    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    channel.basic_consume(queue=TASK_QUEUE, on_message_callback=process_task)

    print(f" [*] Worker {WORKER_ID} started")
    print(" [*] Waiting for tasks. To exit press CTRL+C")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()

    channel.basic_publish(
        exchange='',
        routing_key=WORKER_REGISTRY_QUEUE,
        body=json.dumps({
            'worker_id': WORKER_ID,
            'status': 'shutdown',
            'action': 'deregister',
            'timestamp': datetime.now().isoformat()
        })
    )

    log_event(channel, 'WORKER_STOPPED', {
        'reason': 'user_shutdown'
    })

    connection.close()
    print(f" [x] Worker {WORKER_ID} stopped")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(' [x] Worker interrupted')
        sys.exit(0)
