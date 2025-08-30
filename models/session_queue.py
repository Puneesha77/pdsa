# models/session_queue.py

import time
from collections import deque
from typing import Dict, Optional


class SessionQueue:
    """
    Session Queue for managing user authentication/login events.
    
    Features:
    - FIFO queue of session events (login, logout, reconnect).
    - Each event includes timestamp + status.
    - Prevents overloading the auth system by processing sequentially.
    """

    def __init__(self, max_size: int = 1000):
        self.queue = deque(maxlen=max_size)

    def enqueue(self, user: str, event_type: str) -> Dict:
        """
        Add a session event to the queue.

        Args:
            user: Username or ID
            event_type: "login", "logout", or "reconnect"

        Returns:
            The created event object
        """
        event = {
            "user": user,
            "event_type": event_type,
            "timestamp": time.time(),
            "status": "pending"
        }
        self.queue.append(event)
        print(f"🔑 Enqueued session event: {event}")
        return event

    def dequeue(self) -> Optional[Dict]:
        """
        Process the next session event (FIFO).
        Marks it as processed.

        Returns:
            The processed event or None if queue empty.
        """
        if not self.queue:
            return None

        event = self.queue.popleft()
        event["status"] = "processed"
        print(f"✅ Processed session event: {event}")
        return event

    def peek(self) -> Optional[Dict]:
        """Look at the next event without removing it."""
        if not self.queue:
            return None
        return self.queue[0]

    def size(self) -> int:
        return len(self.queue)

    def clear(self) -> int:
        """Clear all session events."""
        count = len(self.queue)
        self.queue.clear()
        print(f"🧹 Cleared {count} session events")
        return count
