import os
import csv
from datetime import datetime
from collections import deque
import atexit

class EventLogger:
    def __init__(self):
        """
        Initialize the event logger with an output directory and a deque to store events.
        
        Args:
            output_dir (str): Directory path where log_events.csv will be stored
        """
        self.output_dir = 'course_data/'
        self.log_file = os.path.join(self.output_dir, "log_events.csv")
        self.events = deque(maxlen=100)  # Only keep last 100 events in memory
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Clear existing log file if it exists
        open(self.log_file, 'w').close()
        
        # Write CSV header
        with open(self.log_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'event_title', 'event_details'])
        
        # Register the flush_events method to run at program termination
        atexit.register(self.flush_events)
    
    def log_data(self, event_title, event_details):
        """
        Log an event with current UTC timestamp.
        
        Args:
            event_title (str): Title of the event
            event_details (str): Detailed description of the event
        """
        timestamp = datetime.utcnow().isoformat()
        event = (timestamp, event_title, event_details)
        self.events.append(event)
        
        # If we have accumulated enough events, write them to file
        if len(self.events) >= 50:  # Flush after 50 events for better performance
            self.flush_events()
    
    def flush_events(self):
        """
        Write all pending events to the CSV file and clear the events deque.
        """
        if not self.events:
            return
            
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(self.events)
        
        self.events.clear()