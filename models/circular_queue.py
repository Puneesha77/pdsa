"""
Circular Queue for Chat History
Maintains the last N messages efficiently
"""

from collections import deque
from typing import List, Dict, Optional


class CircularQueue:
    """
    A circular queue (fixed-size) for storing message history.
    
    Features:
    - Stores only the last `max_size` messages
    - O(1) enqueue and dequeue operations
    - Prevents unlimited memory growth
    """

    def __init__(self, max_size: int = 10):
        """
        Initialize the circular queue

        Args:
            max_size: Maximum number of messages to store
        """
        self.queue = deque(maxlen=max_size)
        self.max_size = max_size

    def enqueue(self, message: Dict) -> None:
        """
        Add a message to the circular queue (oldest auto-removed if full)

        Args:
            message: Message object (dict)
        """
        self.queue.append(message)
        print(f"🌀 Added to history queue (size={len(self.queue)}/{self.max_size})")

    def get_all(self) -> List[Dict]:
        """
        Get all messages in the history queue

        Returns:
            List of messages in chronological order
        """
        return list(self.queue)

    def latest(self) -> Optional[Dict]:
        """
        Get the most recent message

        Returns:
            Last message dict or None
        """
        if not self.queue:
            return None
        return self.queue[-1]

    def clear(self) -> int:
        """
        Clear all messages from history

        Returns:
            Number of messages cleared
        """
        count = len(self.queue)
        self.queue.clear()
        print(f"🧹 Cleared {count} messages from history queue")
        return count
