import os
import sys
import time
import uuid
import grpc
import argparse
from dotenv import load_dotenv
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from protos import scheduler_pb2
from protos import scheduler_pb2_grpc

root_dir = Path(__file__).resolve().parent.parent
load_dotenv(root_dir / '.env')

SCHEDULER_HOST = os.getenv('SCHEDULER_HOST', 'localhost')
SCHEDULER_PORT = os.getenv('SCHEDULER_PORT', '50051')

def submit_job(execution_time, priority=5, job_id=None):
    if job_id is None:
        job_id = str(uuid.uuid4())

    channel = grpc.insecure_channel(f"{SCHEDULER_HOST}:{SCHEDULER_PORT}")
    stub = scheduler_pb2_grpc.JobSchedulerStub(channel)

    try:
        request = scheduler_pb2.JobRequest(
            job_id=job_id,
            payload=f"Task with execution time {execution_time}s",
            execution_time=execution_time,
            priority=priority
        )

        response = stub.SubmitJob(request)
        print(f"Job submitted successfully: {response.message}")
        return job_id
    except Exception as e:
        print(f"Error submitting job: {e}")
        return None

def check_job_status(job_id):
    channel = grpc.insecure_channel(f"{SCHEDULER_HOST}:{SCHEDULER_PORT}")
    stub = scheduler_pb2_grpc.JobSchedulerStub(channel)

    try:
        request = scheduler_pb2.JobStatusRequest(job_id=job_id)

        # Get status
        response = stub.GetJobStatus(request)
        return response.status
    except Exception as e:
        print(f"Error checking job status: {e}")
        return "error"

def wait_for_completion(job_id, check_interval=2, timeout=300):
    start_time = time.time()
    while time.time() - start_time < timeout:
        status = check_job_status(job_id)

        if status == "completed":
            print(f"Job {job_id} completed successfully!")
            return True
        elif status == "failed":
            print(f"Job {job_id} failed!")
            return False
        elif status == "not_found":
            print(f"Job {job_id} not found!")
            return False

        print(f"Job {job_id} status: {status}. Waiting...")
        time.sleep(check_interval)

    print(f"Timeout waiting for job {job_id}")
    return False

def submit_multiple_jobs(count, min_time, max_time, priority=5):
    """Submit multiple jobs with varying execution times"""
    import random

    job_ids = []
    for i in range(count):
        exec_time = random.randint(min_time, max_time)

        print(f"Submitting job {i+1}/{count} with execution time: {exec_time}s")
        job_id = submit_job(exec_time, priority)

        if job_id:
            job_ids.append(job_id)
            time.sleep(0.2)

    return job_ids

def main():
    parser = argparse.ArgumentParser(description='Client for Distributed Scheduler')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    single_parser = subparsers.add_parser('single', help='Submit a single job')
    single_parser.add_argument('execution_time', type=int, help='Execution time in seconds')
    single_parser.add_argument('--priority', type=int, default=5, help='Job priority (default: 5)')
    single_parser.add_argument('--wait', action='store_true', help='Wait for job completion')

    multi_parser = subparsers.add_parser('multi', help='Submit multiple jobs')
    multi_parser.add_argument('count', type=int, help='Number of jobs to submit')
    multi_parser.add_argument('--min-time', type=int, default=1, help='Minimum execution time (default: 1)')
    multi_parser.add_argument('--max-time', type=int, default=10, help='Maximum execution time (default: 10)')
    multi_parser.add_argument('--priority', type=int, default=5, help='Job priority (default: 5)')

    status_parser = subparsers.add_parser('status', help='Check job status')
    status_parser.add_argument('job_id', help='Job ID to check')

    args = parser.parse_args()

    print(f"Connecting to scheduler at {SCHEDULER_HOST}:{SCHEDULER_PORT}")

    if args.command == 'single':
        print(f"Submitting job with execution time: {args.execution_time}s, priority: {args.priority}")
        job_id = submit_job(args.execution_time, args.priority)

        if job_id and args.wait:
            wait_for_completion(job_id)

    elif args.command == 'multi':
        print(f"Submitting {args.count} jobs with execution times between {args.min_time}s and {args.max_time}s")
        job_ids = submit_multiple_jobs(args.count, args.min_time, args.max_time, args.priority)
        print(f"Submitted {len(job_ids)} jobs successfully")

    elif args.command == 'status':
        status = check_job_status(args.job_id)
        print(f"Job {args.job_id} status: {status}")

    else:
        parser.print_help()

if __name__ == '__main__':
    main()
