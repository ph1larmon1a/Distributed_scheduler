from datetime import datetime

class WorkerState:
    def __init__(self, id, last_heartbeat, status):
        self.id = id
        self.last_heartbeat = last_heartbeat
        self.status = status
