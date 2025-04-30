
import os
import sys
import time
import random
import grpc
import uuid
import argparse
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parent.parent))
from protos import scheduler_pb2
from protos import scheduler_pb2_grpc

from dotenv import load_dotenv
root_dir = Path(__file__).resolve().parent.parent
load_dotenv(root_dir / '.env')

SCHEDULER_HOST = os.getenv('SCHEDULER_HOST', 'localhost')
SCHEDULER_PORT = os.getenv('SCHEDULER_PORT', '50051')
SCHEDULER_ADDRESS = f"{SCHEDULER_HOST}:{SCHEDULER_PORT}"

def generate_random_job():
    """Generate a random job with reasonable parameters"""
    exec_time = random.randint(3, 15)
    priority = random.randint(1, 10)
    job_id = str(uuid.uuid4())

    payload = f"Job {job_id}"

    return {
        'job_id': job_id,
        'execution_time': exec_time,
        'priority': priority,
        'payload': payload
    }

def submit_job(stub, job):
    request = scheduler_pb2.JobRequest(
        job_id=job['job_id'],
        execution_time=job['execution_time'],
        priority=job['priority'],
        payload=job['payload']
    )

    try:
        response = stub.SubmitJob(request)
        print(f"[{datetime.now().isoformat()}] Submitted job {job['job_id']} "
              f"(exec_time={job['execution_time']}s, priority={job['priority']}): {response.message}")
        return job['job_id']
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error submitting job: {e}")
        return None

def check_job_status(stub, job_id):
    request = scheduler_pb2.JobStatusRequest(job_id=job_id)

    try:
        response = stub.GetJobStatus(request)
        return response.status
    except grpc.RpcError as e:
        print(f"Error checking job status: {e}")
        return "error"

def display_job_stats(jobs):
    if not jobs:
        return

    total = len(jobs)
    statuses = {}
    for job_id, status in jobs.items():
        statuses[status] = statuses.get(status, 0) + 1

    print("\n--- Job Statistics ---")
    print(f"Total jobs submitted: {total}")
    for status, count in statuses.items():
        print(f"{status}: {count} ({count/total*100:.1f}%)")
    print("---------------------\n")

def main():
    parser = argparse.ArgumentParser(description="Job generator for distributed scheduler")
    parser.add_argument("interval", type=int, nargs="?", default=5,
                        help="Interval between job submissions in seconds (default: 5)")
    parser.add_argument("--batch", type=int, default=1,
                        help="Number of jobs to submit in each batch (default: 1)")
    parser.add_argument("--total", type=int, default=-1,
                        help="Total number of jobs to submit (default: unlimited)")
    args = parser.parse_args()

    interval = args.interval
    batch_size = args.batch
    total_jobs = args.total

    print(f"Job Generator started:")
    print(f" - Connecting to scheduler at {SCHEDULER_ADDRESS}")
    print(f" - Submitting {batch_size} job(s) every {interval} seconds")
    if total_jobs > 0:
        print(f" - Will submit a total of {total_jobs} jobs")
    else:
        print(f" - Running indefinitely until stopped")

    channel = grpc.insecure_channel(SCHEDULER_ADDRESS)
    stub = scheduler_pb2_grpc.JobSchedulerStub(channel)

    all_jobs = {}
    job_count = 0

    try:
        while True:
            for _ in range(batch_size):
                if total_jobs > 0 and job_count >= total_jobs:
                    print(f"Reached target of {total_jobs} jobs. Stopping.")
                    display_job_stats(all_jobs)
                    return

                job = generate_random_job()
                job_id = submit_job(stub, job)

                if job_id:
                    all_jobs[job_id] = "submitted"
                    job_count += 1

            for job_id in list(all_jobs.keys()):
                if all_jobs[job_id] not in ["completed", "failed", "error"]:
                    status = check_job_status(stub, job_id)
                    if status != all_jobs[job_id]:
                        print(f"Job {job_id[:8]} status: {status}")
                        all_jobs[job_id] = status

            if job_count % (batch_size * 5) == 0:
                display_job_stats(all_jobs)

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nJob generator stopped by user.")
        display_job_stats(all_jobs)
    except Exception as e:
        print(f"Error in job generator: {e}")
    finally:
        channel.close()

if __name__ == "__main__":
    main()
