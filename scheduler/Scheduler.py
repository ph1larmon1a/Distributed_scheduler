import os
import sys
import json
import uuid
import time
import threading
import grpc
import pika
from datetime import datetime
from concurrent import futures
from collections import defaultdict
from dotenv import load_dotenv
from pathlib import Path

from SchedulerState import SchedulerState
from WorkerState import WorkerState
from Job import Job

sys.path.append(str(Path(__file__).resolve().parent.parent))
from protos import scheduler_pb2
from protos import scheduler_pb2_grpc

root_dir = Path(__file__).resolve().parent.parent
load_dotenv(root_dir / '.env')

RMQ_HOST = os.getenv('RMQ_HOST', 'localhost')
RMQ_USER = os.getenv('RMQ_USER', 'guest')
RMQ_PASS = os.getenv('RMQ_PASS', 'guest')
LOGGER_QUEUE = os.getenv('LOGGER_QUEUE', 'system_logs_queue')
HEARTBEAT_QUEUE = os.getenv('HEARTBEAT_QUEUE', 'heartbeat_queue')
TASK_QUEUE = os.getenv('TASK_QUEUE', 'task_queue')
TASK_RESULT_QUEUE = os.getenv('TASK_RESULT_QUEUE', 'task_results')
WORKER_REGISTRY_QUEUE = os.getenv('WORKER_REGISTRY_QUEUE', 'worker_registry')

SCHEDULER_PORT = int(os.getenv('SCHEDULER_PORT', '50051'))
WORKER_TIMEOUT = int(os.getenv('WORKER_TIMEOUT', '120'))
HEARTBEAT_CHECK_INTERVAL = int(os.getenv('HEARTBEAT_CHECK_INTERVAL', '60'))

def create_rabbitmq_connection():
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



class JobSchedulerServicer(scheduler_pb2_grpc.JobSchedulerServicer):
    def __init__(self, state):
        self.state = state

    def SubmitJob(self, request, context):
        job_id = request.job_id if request.job_id else str(uuid.uuid4())
        execution_time = request.execution_time if request.execution_time else 10
        priority = request.priority if request.priority else 5

        job = self.state.add_job(job_id, request.payload, execution_time, priority)

        connection, channel = create_rabbitmq_connection()

        self.log_event(channel, 'JOB_SUBMITTED', {
            'job_id': job_id,
            'execution_time': execution_time,
            'priority': priority
        })

        self.schedule_job(channel, job)

        connection.close()

        return scheduler_pb2.JobResponse(message=f"Job {job_id} submitted successfully")

    def GetJobStatus(self, request, context):
        job_id = request.job_id
        status = self.state.get_job_status(job_id)

        return scheduler_pb2.JobStatusResponse(status=status)

    def schedule_job(self, channel, job: Job):
        try:
            self.state.update_job_status(job.id, 'queued')

            task_message = json.dumps({
                'task_id': job.id,
                'execution_time': job.execution_time,
                'priority': job.priority,
                'payload': job.payload,
                'timestamp': datetime.now().isoformat()
            })

            channel.basic_publish(
                exchange='',
                routing_key=TASK_QUEUE,
                body=task_message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    priority=job.priority
                )
            )

            self.log_event(channel, 'TASK_SCHEDULED', {
                'task_id': job.id,
                'execution_time': job.execution_time,
                'priority': job.priority
            })

            print(f"Job {job.id} scheduled for execution")
            return True
        except Exception as e:
            print(f"Error scheduling job: {e}")
            self.state.update_job_status(job.id, 'scheduling_failed')
            return False

    def log_event(self, channel, event_type, details):
        try:
            channel.basic_publish(
                exchange='',
                routing_key=LOGGER_QUEUE,
                body=json.dumps({
                    'event_type': event_type,
                    'component': 'scheduler',
                    'details': details,
                    'timestamp': datetime.now().isoformat()
                }),
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
        except Exception as e:
            print(f"Error sending log event: {e}")


def rabbitmq_listener(scheduler_state):
    def setup_listener_connection():
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

            channel.basic_qos(prefetch_count=100)

            return connection, channel
        except Exception as e:
            print(f"Error setting up RabbitMQ listener connection: {e}")
            time.sleep(5)
            return None, None

    def on_worker_registry(ch, method, properties, body):
        try:
            data = json.loads(body)
            worker_id = data.get('worker_id')
            action = data.get('action')
            capacity = data.get('capacity', 1)

            if action == 'register':
                scheduler_state.register_worker(worker_id, capacity)
                print(f"Worker {worker_id} registered")
            elif action == 'deregister':
                scheduler_state.delete_worker(worker_id)
                print(f"Worker {worker_id} deregistered")

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Error processing worker registry message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag)

    def on_worker_heartbeat(ch, method, properties, body):
        try:
            data = json.loads(body)
            worker_id = data.get('worker_id')

            scheduler_state.update_worker_heartbeat(worker_id)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f"Error processing heartbeat: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def on_task_result(ch, method, properties, body):
        try:
            data = json.loads(body)
            task_id = data.get('task_id')
            worker_id = data.get('worker_id')
            status = data.get('status')

            if status == 'completed':
                scheduler_state.update_job_status(task_id, 'completed', worker_id)
                print(f"Task {task_id} completed by worker {worker_id}")
            elif status == 'failed':
                scheduler_state.update_job_status(task_id, 'failed', worker_id)
                print(f"Task {task_id} failed by worker {worker_id}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Error processing task result: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag)

    while True:
        try:
            connection, channel = setup_listener_connection()
            if not connection or not channel:
                continue

            print("RabbitMQ listener thread started, setting up consumers...")

            channel.basic_consume(
                queue=WORKER_REGISTRY_QUEUE,
                on_message_callback=on_worker_registry
            )

            channel.basic_consume(
                queue=HEARTBEAT_QUEUE,
                on_message_callback=on_worker_heartbeat
            )

            channel.basic_consume(
                queue=TASK_RESULT_QUEUE,
                on_message_callback=on_task_result
            )

            print("All consumers registered, starting to consume messages...")

            channel.start_consuming()

        except pika.exceptions.ConnectionClosedByBroker:
            print("Connection was closed by broker, retrying...")
            time.sleep(5)
            continue

        except pika.exceptions.AMQPChannelError as e:
            print(f"Channel error: {e}, retrying...")
            time.sleep(5)
            continue

        except pika.exceptions.AMQPConnectionError:
            print("Connection was lost, retrying...")
            time.sleep(5)
            continue

        except Exception as e:
            print(f"Unexpected error in RabbitMQ listener: {e}")
            time.sleep(5)
            continue

        finally:
            try:
                if channel and channel.is_open:
                    channel.stop_consuming()
                if connection and connection.is_open:
                    connection.close()
                print("RabbitMQ listener connection closed")
            except Exception as e:
                print(f"Error closing RabbitMQ connection: {e}")


def worker_health_check(scheduler_state):
    while True:
        try:
            connection, channel = create_rabbitmq_connection()

            scheduler_state.check_workers_health()
            active_workers = scheduler_state.get_active_workers()
            print(f"Active workers: {len(active_workers)}")

            channel.basic_publish(
                exchange='',
                routing_key=LOGGER_QUEUE,
                body=json.dumps({
                    'event_type': 'WORKER_STATS',
                    'component': 'scheduler',
                    'details': {
                        'active_workers': len(active_workers),
                        'worker_ids': [w.id for w in active_workers]
                    },
                    'timestamp': datetime.now().isoformat()
                })
            )

            connection.close()

        except Exception as e:
            print(f"Error checking worker health: {e}")

        time.sleep(HEARTBEAT_CHECK_INTERVAL)


def flush_heartbeat_queue():
    try:
        connection, channel = create_rabbitmq_connection()
        channel.queue_purge(queue=HEARTBEAT_QUEUE)
        print(f"Purged heartbeat queue")
        connection.close()
    except Exception as e:
        print(f"Error purging heartbeat queue: {e}")


def main():
    scheduler_state = SchedulerState()
    connection, channel = create_rabbitmq_connection()

    flush_heartbeat_queue()

    channel.basic_publish(
        exchange='',
        routing_key=LOGGER_QUEUE,
        body=json.dumps({
            'event_type': 'SCHEDULER_STARTED',
            'component': 'scheduler',
            'details': {
                'port': SCHEDULER_PORT
            },
            'timestamp': datetime.now().isoformat()
        })
    )

    connection.close()

    listener_thread = threading.Thread(target=rabbitmq_listener, args=(scheduler_state,), daemon=True)
    listener_thread.start()

    health_check_thread = threading.Thread(
        target=worker_health_check,
        args=(scheduler_state,),
        daemon=True
    )
    health_check_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    scheduler_pb2_grpc.add_JobSchedulerServicer_to_server(
        JobSchedulerServicer(scheduler_state), server
    )
    server.add_insecure_port(f'[::]:{SCHEDULER_PORT}')
    server.start()

    print(f"Scheduler started on port {SCHEDULER_PORT}")
    print("Waiting for jobs and worker connections...")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nShutting down scheduler...")

        connection, channel = create_rabbitmq_connection()

        channel.basic_publish(
            exchange='',
            routing_key=LOGGER_QUEUE,
            body=json.dumps({
                'event_type': 'SCHEDULER_STOPPED',
                'component': 'scheduler',
                'details': {
                    'reason': 'user_shutdown'
                },
                'timestamp': datetime.now().isoformat()
            })
        )

        connection.close()
        server.stop(0)


if __name__ == "__main__":
    main()
