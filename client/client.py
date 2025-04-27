import grpc
import scheduler_pb2
import scheduler_pb2_grpc

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = scheduler_pb2_grpc.JobSchedulerStub(channel)

        job = scheduler_pb2.JobRequest(
            job_id="job1",
            task="process_data",
            data="Sample payload",
            priority=1
        )

        response = stub.SubmitJob(job)
        print(f"[Client] Server Response: {response.message}")

if __name__ == "__main__":
    run()