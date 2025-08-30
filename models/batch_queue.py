# models/batch_queue.py

from collections import deque
from typing import List, Dict

class BatchQueue:
    """
    Batch Queue (FIFO) for grouping messages before network transmission.
    
    Features:
    - Collects messages into batches of size N (default 10).
    - Supports manual flush or auto flush when threshold reached.
    - Ensures network efficiency by sending fewer, larger packets.
    """

    def __init__(self, min_batch_size: int = 5, max_batch_size: int = 50):
        self.queue = deque()
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size

    def enqueue(self, message: Dict) -> None:
        """Add a message to the batch queue"""
        self.queue.append(message)
        print(f"📦 Added message to batch queue (size={len(self.queue)})")

    def flush(self) -> List[Dict]:
        """
        Flush messages as a batch.
        Returns a list of messages to send.
        """
        if len(self.queue) < self.min_batch_size:
            return []  # Not enough messages yet

        batch = []
        while self.queue and len(batch) < self.max_batch_size:
            batch.append(self.queue.popleft())

        print(f"🚀 Flushed batch of {len(batch)} messages")
        return batch

    def force_flush(self) -> List[Dict]:
        """Force flush all messages regardless of size"""
        batch = list(self.queue)
        self.queue.clear()
        print(f"⚡ Force flushed {len(batch)} messages")
        return batch

    def size(self) -> int:
        return len(self.queue)
