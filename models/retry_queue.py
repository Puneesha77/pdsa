# models/retry_queue.py

import time
from collections import deque
from typing import Dict, Optional, Tuple

class RetryQueue:
    """
    Retry Queue for handling failed messages with exponential backoff.
    
    Features:
    - Failed messages are enqueued with retry count.
    - Waits exponentially longer before retrying each time.
    - Ensures reliable delivery even with temporary network issues.
    """

    def __init__(self, max_retries: int = 5):
        self.queue = deque()
        self.max_retries = max_retries

    def enqueue(self, message: Dict, retry_count: int = 0) -> None:
        """Add a failed message to retry queue"""
        next_time = time.time() + (2 ** retry_count)  # exponential backoff
        self.queue.append((message, retry_count, next_time))
        print(f"🔄 Queued message for retry (attempt {retry_count + 1})")

    def get_ready_message(self) -> Optional[Tuple[Dict, int]]:
        """
        Get a message ready for retry if its backoff time has passed.
        Returns (message, retry_count) or None if none ready.
        """
        if not self.queue:
            return None

        message, retry_count, next_time = self.queue[0]
        if time.time() >= next_time:
            self.queue.popleft()
            return message, retry_count
        return None

    def process(self) -> Optional[Dict]:
        """
        Process one retry if available.
        Returns the message or None.
        """
        ready = self.get_ready_message()
        if not ready:
            return None

        message, retry_count = ready
        if retry_count < self.max_retries:
            return message
        else:
            print("❌ Dropped message after max retries")
            return None

    def size(self) -> int:
        return len(self.queue)
