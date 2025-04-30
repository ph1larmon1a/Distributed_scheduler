from datetime import datetime

class Job:
    def __init__(self, job_id, payload, execution_time, priority=5):
        self.id = job_id
        self.payload = payload
        self.execution_time = execution_time
        self.priority = priority
        self.status = 'pending'
        self.updated_at = datetime.now()
        self.created_at = datetime.now()
        self.worker_id = None
