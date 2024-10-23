import threading
import queue
import time
import os

# Placeholder function for folder monitoring and processing logic
def monitor_folder(path, job_queue):
    # Simulate finding a job folder
    jobs = os.listdir(path)
    for job in jobs:
        priority = get_job_priority(job)
        job_queue.put((priority, job))

# Determine priority based on job name
def get_job_priority(job_name):
    if 'DLE' in job_name:
        return 1
    elif '_MR_' in job_name:
        return 2
    else:
        return 3

def process_job(service_name, job_queue, lock):
    while True:
        _, job = job_queue.get()  # fetch job with highest priority
        with lock:
            print(f"{service_name}: Processing {job}")
            # Simulate processing time
            time.sleep(2)
            complete_processing(job)
            job_queue.task_done()

def complete_processing(job):
    # Placeholder for the actual folder transformation logic
    print(f"Completed processing {job}")

# Main function to start threads simulating services
def main():
    max_executors = 5
    lock = threading.Lock()

    # Priority queue to manage jobs based on priority
    job_queue = queue.PriorityQueue()

    # Simulate folder paths for each service
    extract_path = 'extract.started'
    hyper_path = 'hyper.pending'
    publish_path = 'publish.pending'

    # Start processing threads
    for _ in range(max_executors):
        threading.Thread(target=process_job, args=('Extract', job_queue, lock), daemon=True).start()
        threading.Thread(target=process_job, args=('Hyper', job_queue, lock), daemon=True).start()
        threading.Thread(target=process_job, args=('Publish', job_queue, lock), daemon=True).start()

    # Monitor folders in separate threads
    threading.Thread(target=monitor_folder, args=(extract_path, job_queue), daemon=True).start()
    threading.Thread(target=monitor_folder, args=(hyper_path, job_queue), daemon=True).start()
    threading.Thread(target=monitor_folder, args=(publish_path, job_queue), daemon=True).start()

    # Keep the main thread alive to keep daemon threads running
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
