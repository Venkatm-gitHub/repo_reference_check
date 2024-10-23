import os
import threading
import queue
import heapq
import fnmatch

# Define the priority queue
priority_queue = []

# Define the priority assignment function
def assign_priority(job_name):
    if fnmatch.fnmatch(job_name, '*DLE*'):
        return 1
    elif fnmatch.fnmatch(job_name, '*_MR_*'):
        return 2
    else:
        return 3

# Define the job processing function
def process_job(job_name):
    # Get the priority of the job
    priority = assign_priority(job_name)

    # Add the job to the priority queue
    heapq.heappush(priority_queue, (priority, job_name))

    # Process the job
    while True:
        # Check if the job is at the top of the priority queue
        if priority_queue[0][1] == job_name:
            # Process the job
            # ... (existing code)

            # Remove the job from the priority queue
            heapq.heappop(priority_queue)
            break

        # If the job is not at the top of the priority queue, wait
        else:
            # ... (existing code)

# Define the thread function
def thread_function():
    while True:
        # Get the next job from the queue
        job_name = queue.get()

        # Process the job
        process_job(job_name)

        # Mark the job as done
        queue.task_done()

# Create the threads
threads = []
for i in range(5):
    thread = threading.Thread(target=thread_function)
    thread.start()
    threads.append(thread)

# Monitor the folders and add jobs to the queue
while True:
    # ... (existing code)

    # Add the job to the queue
    queue.put(job_name)