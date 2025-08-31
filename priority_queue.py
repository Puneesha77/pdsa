"""
Priority Queue Model for Chat System

Handles priority queue operations, message management, history,
and offline message creation.
"""

import heapq
import time
from typing import Dict, List, Optional
from models.circular_queue import CircularQueue


class PriorityMessageQueue:
    """
    A priority-based message queue system for real-time chat

    Features:
    - Priority ordering (1=URGENT, 2=HIGH, 3=NORMAL, 4=LOW)
    - Automatic priority detection based on keywords/heuristics
    - Message history storage via CircularQueue (default max 10)
    - FIFO ordering for same-priority messages
    - Offline message creation support
    """

    def __init__(self, max_history_size: int = 10):
        """
        Initialize the priority message queue

        Args:
            max_history_size: Maximum number of messages to keep in history
        """
        self.priority_queue: List = []  # Min-heap for priority ordering
        self.history = CircularQueue(max_size=max_history_size)  # Rolling history
        self.message_counter = 0  # Ensures FIFO for same priority

        # Priority configuration
        self.PRIORITY_NAMES = {
            1: "URGENT",
            2: "HIGH",
            3: "NORMAL",
            4: "LOW"
        }

        # Keywords for auto-detection
        self.URGENT_KEYWORDS = [
            'urgent', 'emergency', 'help', 'asap', '911',
            'critical', 'bug', 'down', 'broken', 'crash'
        ]

        self.HIGH_KEYWORDS = [
            'important', 'priority', 'meeting', 'deadline',
            'issue', 'attention', 'review', 'approval'
        ]

    # ------------------------------
    # Public API
    # ------------------------------

    def add_message(self, message: str, user: str = "Anonymous",
                    manual_priority: Optional[int] = None) -> Dict:
        """
        Add a message to the priority queue

        Args:
            message: The message text
            user: Username of sender
            manual_priority: Override auto-detection (1-4, None for auto)

        Returns:
            Dict containing the created message object
        """
        self.message_counter += 1
        timestamp = time.time()

        # Determine priority
        if manual_priority is not None:
            priority = int(manual_priority)
            detection_method = "manual"
        else:
            priority = self.auto_detect_priority(message)
            detection_method = "auto"

        # Create message object
        message_obj = {
            'text': message,
            'priority': priority,
            'priority_name': self.get_priority_name(priority),
            'user': user,
            'timestamp': timestamp,
            'counter': self.message_counter,
            'detection_method': detection_method
        }

        # Add to active priority queue (min-heap)
        heapq.heappush(self.priority_queue, (priority, self.message_counter, message_obj))

        # Add to circular history queue (auto-truncates)
        self.history.enqueue(message_obj)

        print(f"📥 [{detection_method.upper()}] Added {self.get_priority_name(priority)} priority message")
        print(f"🔢 Active queue size: {len(self.priority_queue)} | History size: {len(self.history.get_all())}")

        return message_obj

    def get_next_message(self) -> Optional[Dict]:
        """Get and remove the highest priority message from queue"""
        if not self.priority_queue:
            return None

        _, _, message_obj = heapq.heappop(self.priority_queue)
        print(f"📤 Delivering {message_obj['priority_name']} message: {message_obj['text'][:50]}...")
        return message_obj

    def peek_next_message(self) -> Optional[Dict]:
        """Look at the highest priority message without removing it"""
        if not self.priority_queue:
            return None
        return self.priority_queue[0][2]

    def get_history(self) -> List[Dict]:
        """Get last N messages from history"""
        return self.history.get_all()

    def get_queue_stats(self) -> Dict:
        """Get statistics about the current queue state"""
        stats = {
            'total_messages': len(self.priority_queue),
            'history_size': len(self.history.get_all()),
            'priority_breakdown': {}
        }

        for priority, _, _ in self.priority_queue:
            name = self.get_priority_name(priority)
            stats['priority_breakdown'][name] = stats['priority_breakdown'].get(name, 0) + 1

        return stats

    def clear_queue(self) -> int:
        """Clear only the active priority queue (keeps history)"""
        cleared_count = len(self.priority_queue)
        self.priority_queue.clear()
        print(f"🧹 Cleared {cleared_count} messages from active priority queue")
        return cleared_count

    def clear_all(self) -> Dict:
        """Clear both active queue and history"""
        queue_count = len(self.priority_queue)
        history_count = len(self.history.get_all())

        self.priority_queue.clear()
        self.history.clear()
        self.message_counter = 0

        print(f"🧹 Cleared everything: {queue_count} active queue, {history_count} history")
        return {'queue_cleared': queue_count, 'history_cleared': history_count}

    def create_offline_message(self, message: str, user: str, recipient: str,
                               manual_priority: Optional[int] = None) -> Dict:
        """
        Create a message object for offline storage (without adding to queue)
        """
        self.message_counter += 1
        timestamp = time.time()

        if manual_priority is not None:
            priority = int(manual_priority)
            detection_method = "manual"
        else:
            priority = self.auto_detect_priority(message)
            detection_method = "auto"

        message_obj = {
            'text': message,
            'priority': priority,
            'priority_name': self.get_priority_name(priority),
            'user': user,
            'recipient': recipient,
            'timestamp': timestamp,
            'counter': self.message_counter,
            'detection_method': detection_method,
            'is_offline_message': True
        }

        print(f"📦 Created offline message for {recipient} from {user}")
        return message_obj

    # ------------------------------
    # Helpers
    # ------------------------------

    def auto_detect_priority(self, message: str) -> int:
        """Automatically detect message priority based on content"""
        message_lower = message.lower().strip()

        if any(keyword in message_lower for keyword in self.URGENT_KEYWORDS):
            return 1  # URGENT
        if any(keyword in message_lower for keyword in self.HIGH_KEYWORDS):
            return 2  # HIGH
        if '@' in message and len(message) > 1:
            return 2  # HIGH
        if len(message) > 5 and message.isupper():
            return 2  # HIGH
        if message.count('!') >= 3:
            return 2  # HIGH

        return 3  # NORMAL

    def get_priority_name(self, priority: int) -> str:
        """Convert priority number to human-readable name"""
        return self.PRIORITY_NAMES.get(priority, "NORMAL")
