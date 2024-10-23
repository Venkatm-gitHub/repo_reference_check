import os
import queue
import threading

# Priority levels
HIGH_PRIORITY = 1  # for DLE jobs
MEDIUM_PRIORITY = 2  # for MR jobs
LOW_PRIORITY = 3  # for all other jobs

class Job:
    def __init__(self, job_name, folder_path):
        self.job_name = job_name
        self.folder_path = folder_path
        self.priority = self.get_priority()

    def get_priority(self):
        if "DLE" in self.job_name:
            return HIGH_PRIORITY
        elif "MR" in self.job_name:
            return MEDIUM_PRIORITY
        else:
            return LOW_PRIORITY

class Service(threading.Thread):
    def __init__(self, service_name, folder_to_monitor, job_queue):
        threading.Thread.__init__(self)
        self.service_name = service_name
        self.folder_to_monitor = folder_to_monitor
        self.job_queue = job_queue

    def run(self):
        while True:
            # Monitor the folder for new jobs
            for folder in os.listdir(self.folder_to_monitor):
                job = Job(job_name=folder, folder_path=os.path.join(self.folder_to_monitor, folder))
                
                # Add job to priority queue based on its priority
                self.job_queue.put((job.priority, job))
            
            # Process the jobs
            priority, job = self.job_queue.get()
            self.process_job(job)

    def process_job(self, job):
        print(f"Processing job: {job.job_name} in service: {self.service_name}")
        # Add the actual processing logic here, such as moving files between folders.

class ExecutorPool:
    def __init__(self, num_executors=5):
        self.num_executors = num_executors
        self.lock = threading.Lock()
        self.current_executors = 0

    def acquire_executor(self):
        with self.lock:
            if self.current_executors < self.num_executors:
                self.current_executors += 1
                return True
            return False

    def release_executor(self):
        with self.lock:
            self.current_executors -= 1

def main():
    # Create a priority queue
    job_queue = queue.PriorityQueue()

    # Initialize the services
    extract_service = Service(service_name="extract", folder_to_monitor="extract.started", job_queue=job_queue)
    hyper_service = Service(service_name="hyper", folder_to_monitor="extract.pending", job_queue=job_queue)
    publish_service = Service(service_name="publish", folder_to_monitor="hyper.pending", job_queue=job_queue)

    # Initialize the executor pool
    executor_pool = ExecutorPool()

    # Start the services
    extract_service.start()
    hyper_service.start()
    publish_service.start()

if __name__ == "__main__":
    main()
