import grpc
from concurrent import futures
import time
import pika
import scheduler_pb2
import scheduler_pb2_grpc

# RabbitMQ setup
RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'jobs'

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME)

class JobSchedulerServicer(scheduler_pb2_grpc.JobSchedulerServicer):
    def SubmitJob(self, request, context):
        job = {
            'job_id': request.job_id,
            'task': request.task,
            'data': request.data,
            'priority': request.priority
        }
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=str(job)
        )
        print(f"[Scheduler] Job submitted: {job}")
        return scheduler_pb2.JobResponse(message="Job received and queued.")

    def GetJobStatus(self, request, context):
        # Stubbed response for now
        return scheduler_pb2.JobStatusResponse(status="Pending")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    scheduler_pb2_grpc.add_JobSchedulerServicer_to_server(JobSchedulerServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("[Scheduler] gRPC server running on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
