import queue
import threading
import os
import re
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict
from dataclasses import dataclass

class JobPriority(Enum):
    HIGH = 1    # For *DLE* pattern
    MEDIUM = 2  # For *MR* pattern
    LOW = 3     # For other jobs

@dataclass
class Job:
    name: str
    timestamp: datetime
    priority: JobPriority
    current_stage: str
    content_path: str

class PriorityQueueManager:
    def __init__(self, max_executors: int = 5):
        # Priority queues for different stages
        self.extract_queue = queue.PriorityQueue()
        self.hyper_queue = queue.PriorityQueue()
        self.publish_queue = queue.PriorityQueue()
        
        # Tracking active jobs
        self.active_jobs: Dict[str, Job] = {}
        self.active_jobs_lock = threading.Lock()
        
        # Executor management
        self.max_executors = max_executors
        self.available_executors = threading.Semaphore(max_executors)
        
        # DLE jobs tracking for hyper stage
        self.dle_jobs_in_hyper = threading.Event()
        
    def get_job_priority(self, job_name: str) -> JobPriority:
        """Determine job priority based on patterns"""
        if re.search(r'DLE', job_name):
            return JobPriority.HIGH
        elif re.search(r'MR', job_name):
            return JobPriority.MEDIUM
        return JobPriority.LOW

    def can_process_job(self, job: Job) -> bool:
        """Check if a job can be processed based on priority rules"""
        if job.current_stage == 'hyper':
            # If there's a DLE job in hyper, only allow other DLE jobs
            if self.dle_jobs_in_hyper.is_set() and job.priority != JobPriority.HIGH:
                return False
        return True

    def add_job(self, job_name: str, stage: str):
        """Add a new job to the appropriate queue"""
        priority = self.get_job_priority(job_name)
        job = Job(
            name=job_name,
            timestamp=datetime.now(),
            priority=priority,
            current_stage=stage,
            content_path=f"{stage}.started/{job_name}"
        )
        
        # Priority tuple: (priority.value, timestamp) ensures FIFO within same priority
        priority_tuple = (priority.value, job.timestamp)
        
        if stage == 'extract':
            self.extract_queue.put((priority_tuple, job))
        elif stage == 'hyper':
            self.hyper_queue.put((priority_tuple, job))
        elif stage == 'publish':
            self.publish_queue.put((priority_tuple, job))

    def process_job(self, stage: str):
        """Process jobs from the queue for a specific stage"""
        queue_map = {
            'extract': self.extract_queue,
            'hyper': self.hyper_queue,
            'publish': self.publish_queue
        }
        
        current_queue = queue_map[stage]
        
        while True:
            # Wait for available executor
            self.available_executors.acquire()
            
            try:
                # Get next job from queue
                _, job = current_queue.get(block=True)
                
                # Check if job can be processed based on priority rules
                if not self.can_process_job(job):
                    # Put job back in queue and wait
                    current_queue.put((job.priority.value, job))
                    self.available_executors.release()
                    continue
                
                # Track DLE jobs in hyper stage
                if stage == 'hyper' and job.priority == JobPriority.HIGH:
                    self.dle_jobs_in_hyper.set()
                
                try:
                    # Process the job
                    success = self._process_stage(job)
                    
                    if success:
                        self._move_to_next_stage(job)
                    else:
                        self._move_to_failed(job)
                        
                finally:
                    # Cleanup
                    if stage == 'hyper' and job.priority == JobPriority.HIGH:
                        # Check if there are more DLE jobs in hyper queue
                        has_more_dle = any(j.priority == JobPriority.HIGH 
                                         for _, j in list(self.hyper_queue.queue))
                        if not has_more_dle:
                            self.dle_jobs_in_hyper.clear()
                    
                    self.available_executors.release()
                    
            except Exception as e:
                print(f"Error processing job in {stage}: {str(e)}")
                self.available_executors.release()

    def _process_stage(self, job: Job) -> bool:
        """
        Process a specific stage for a job
        Returns True if successful, False otherwise
        """
        try:
            # Implementation specific to each stage
            if job.current_stage == 'extract':
                # Extract processing logic
                pass
            elif job.current_stage == 'hyper':
                # Hyper processing logic
                pass
            elif job.current_stage == 'publish':
                # Publish processing logic
                pass
            return True
        except Exception:
            return False

    def _move_to_next_stage(self, job: Job):
        """Move job contents to next stage"""
        stage_flow = {
            'extract': 'hyper',
            'hyper': 'publish',
            'publish': 'completed'
        }
        
        next_stage = stage_flow.get(job.current_stage)
        if next_stage:
            # Move contents to next stage's pending folder
            source = job.content_path
            destination = f"{next_stage}.pending/{job.name}"
            # Implement actual file movement logic here
            
            # Add to next queue if not completed
            if next_stage != 'completed':
                job.current_stage = next_stage
                job.content_path = destination
                self.add_job(job.name, next_stage)

    def _move_to_failed(self, job: Job):
        """Move job contents to failed folder for current stage"""
        failed_path = f"{job.current_stage}.failed/{job.name}"
        # Implement actual file movement logic here

# Example usage
def start_services():
    manager = PriorityQueueManager(max_executors=5)
    
    # Start service threads
    services = ['extract', 'hyper', 'publish']
    threads = []
    
    for service in services:
        thread = threading.Thread(
            target=manager.process_job,
            args=(service,),
            daemon=True
        )
        thread.start()
        threads.append(thread)
    
    # Monitor folders and add jobs
    def monitor_folders():
        while True:
            # Implement folder monitoring logic here
            # When new folder is detected:
            # manager.add_job(job_name, stage)
            pass
    
    monitor_thread = threading.Thread(target=monitor_folders, daemon=True)
    monitor_thread.start()