import threading
import queue
import time

class Job:
    def __init__(self, name):
        self.name = name

class JobProcessor:
    def __init__(self):
        self.extract_queue = queue.Queue()
        self.hyper_queue = queue.Queue()
        self.publish_queue = queue.Queue()
        self.max_executors = 5
        self.current_executors = 0
        self.dle_running = False  # Flag to track if a DLE job is running

    def monitor_folders(self):
        while True:
            if not self.extract_queue.empty():
                self.process_extract_jobs()
            time.sleep(1)

    def process_extract_jobs(self):
        if self.current_executors < self.max_executors:
            job = self.extract_queue.get()
            self.current_executors += 1
            threading.Thread(target=self.handle_extract, args=(job,)).start()

    def handle_extract(self, job):
        print(f'Starting extract for {job.name}')
        time.sleep(2)  # Simulate job processing time
        
        # Check for DLE jobs
        if 'DLE' in job.name:
            self.dle_running = True  # Set flag indicating a DLE job is running
            self.hyper_queue.put(job)  # Prioritize DLE jobs to hyper queue
        elif '_MR_' in job.name:
            if self.dle_running:  # If a DLE job is running, re-queue MR jobs
                print(f'Re-queuing MR job {job.name} due to DLE priority')
                self.extract_queue.put(job)
            else:
                self.hyper_queue.put(job)  # Process MR jobs if no DLE jobs are running
        else:
            self.extract_queue.put(job)  # Normal processing for other jobs
        
        # Simulate moving to hyper processing
        time.sleep(1)  # Simulate hyper processing delay
        
        # Reset the DLE flag if finished processing a DLE job
        if 'DLE' in job.name:
            print(f'Finished processing DLE job {job.name}')
            self.dle_running = False
        
        self.current_executors -= 1

# Example usage
job_processor = JobProcessor()
threading.Thread(target=job_processor.monitor_folders).start()

# Simulate adding jobs to the extract queue
job_processor.extract_queue.put(Job('DLE_123'))
job_processor.extract_queue.put(Job('MR_456'))
job_processor.extract_queue.put(Job('NORMAL_789'))