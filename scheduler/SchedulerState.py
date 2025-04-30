from WorkerState import WorkerState
from Job import Job
from datetime import datetime

import sys
import threading
import logging

import os
from pathlib import Path
from dotenv import load_dotenv

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

class SchedulerState:
    def __init__(self):
        self.workers = {}
        self.jobs = {}
        self.lock = threading.Lock()

    def register_worker(self, worker_id, capacity=1):
        with self.lock:
            self.workers[worker_id] = WorkerState(worker_id, datetime.now(), 'active')
            return True

    def update_worker_heartbeat(self, worker_id):
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id].last_heartbeat = datetime.now()
                self.workers[worker_id].status = 'active'
                return True
            return False

    def delete_worker(self, worker_id):
        with self.lock:
            if worker_id in self.workers:
                del self.workers[worker_id]
                return True
            return False

    def check_workers_health(self):
        now = datetime.now()
        with self.lock:
            inactive_workers = []
            for worker_id, info in self.workers.items():
                time_diff = (now - info.last_heartbeat).total_seconds()
                if time_diff > WORKER_TIMEOUT:
                    inactive_workers.append(worker_id)

            for worker_id in inactive_workers:
                logging.info(f"Worker {worker_id} considered dead (no heartbeat)")
                self.workers[worker_id].status = 'inactive'

    def get_active_workers(self):
        with self.lock:
            return [w for w in self.workers.values() if w.status == 'active']

    def add_job(self, job_id, payload, execution_time, priority=1):
        with self.lock:
            self.jobs[job_id] = Job(job_id, payload, execution_time, priority)

            return self.jobs[job_id]

    def update_job_status(self, job_id, status, worker_id=None):
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id].status = status
                self.jobs[job_id].updated_at = datetime.now().isoformat()
                if worker_id:
                    self.jobs[job_id].worker_id = worker_id
                return True
            return False

    def get_job_status(self, job_id):
        with self.lock:
            if job_id in self.jobs:
                return self.jobs[job_id].status
            return 'not_found'

    def get_job(self, job_id):
        with self.lock:
            if job_id in self.jobs:
                return self.jobs[job_id]
            return None
