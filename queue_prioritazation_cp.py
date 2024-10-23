import os
import time
import threading
from queue import PriorityQueue

class Job:
    def __init__(self, name, priority):
        self.name = name
        self.priority = priority

    def __lt__(self, other):
        return self.priority < other.priority

class Service:
    def __init__(self, name, queue):
        self.name = name
        self.queue = queue
        self.running = False

    def monitor_folder(self, folder):
        while True:
            for job_name in os.listdir(folder):
                if not any(job_name in item[2].name for item in self.queue.queue):
                    self.queue.put((self.get_priority(job_name), time.time(), Job(job_name, self.get_priority(job_name))))
            time.sleep(1)

    def get_priority(self, job_name):
        if "DLE" in job_name:
            return 1
        elif "_MR_" in job_name:
            return 2
        else:
            return 3

    def process_job(self):
        while True:
            if not self.running:
                priority, _, job = self.queue.get()
                self.running = True
                print(f"{self.name} is processing {job.name} with priority {priority}")
                time.sleep(2)  # Simulate job processing time
                self.running = False
                self.queue.task_done()
            else:
                time.sleep(1)

def main():
    extract_queue = PriorityQueue()
    hyper_queue = PriorityQueue()
    publish_queue = PriorityQueue()

    extract_service = Service('Extract', extract_queue)
    hyper_service = Service('Hyper', hyper_queue)
    publish_service = Service('Publish', publish_queue)

    # Simulate folders with jobs
    os.makedirs('extract.started', exist_ok=True)
    os.makedirs('extract.pending', exist_ok=True)
    os.makedirs('extract.failed', exist_ok=True)
    os.makedirs('hyper.pending', exist_ok=True)
    os.makedirs('hyper.failed', exist_ok=True)
    os.makedirs('publish.pending', exist_ok=True)
    os.makedirs('publish.completed', exist_ok=True)
    os.makedirs('publish.failed', exist_ok=True)

    # Start monitoring folders
    threading.Thread(target=extract_service.monitor_folder, args=('extract.started',)).start()
    threading.Thread(target=hyper_service.monitor_folder, args=('extract.pending',)).start()
    threading.Thread(target=publish_service.monitor_folder, args=('hyper.pending',)).start()

    # Start processing jobs
    threading.Thread(target=extract_service.process_job).start()
    threading.Thread(target=hyper_service.process_job).start()
    threading.Thread(target=publish_service.process_job).start()

if __name__ == "__main__":
    main()
