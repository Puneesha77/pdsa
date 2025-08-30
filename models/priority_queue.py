"""
Priority Queue Model for Chat System

Handles all priority queue operations and message management
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
    - Automatic priority detection based on keywords
    - Message history storage (circular queue, max 1000 by default)
    - FIFO ordering for same-priority messages
    """

    def __init__(self, max_history_size: int = 1000):
        """
        Initialize the priority message queue

        Args:
            max_history_size: Maximum number of messages to keep in history
        """
        self.priority_queue = []  # Min-heap for priority ordering
        self.history = CircularQueue(max_size=max_history_size)  # Circular queue for history
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
        if manual_priority:
            priority = manual_priority
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

        # Add to priority queue (heapq uses min-heap)
        # Format: (priority, counter, message_obj)
        heapq.heappush(
            self.priority_queue,
            (priority, self.message_counter, message_obj)
        )

        # Add to circular history queue
        self.history.enqueue(message_obj)

        print(f"📥 [{detection_method.upper()}] Added {self.get_priority_name(priority)} priority message")
        print(f"🔢 Queue size: {len(self.priority_queue)} | History size: {len(self.history.get_all())}")

        return message_obj

    def get_next_message(self) -> Optional[Dict]:
        """
        Get and remove the highest priority message from queue

        Returns:
            Message object with highest priority, or None if queue empty
        """
        if not self.priority_queue:
            return None

        priority, counter, message_obj = heapq.heappop(self.priority_queue)

        print(f"📤 Delivering {message_obj['priority_name']} message: {message_obj['text'][:50]}...")
        return message_obj

    def peek_next_message(self) -> Optional[Dict]:
        """
        Look at the highest priority message without removing it

        Returns:
            Message object with highest priority, or None if queue empty
        """
        if not self.priority_queue:
            return None

        priority, counter, message_obj = self.priority_queue[0]
        return message_obj

    def get_history(self) -> List[Dict]:
        """
        Get all messages from history

        Returns:
            List of message objects in chronological order
        """
        return self.history.get_all()

    def get_queue_stats(self) -> Dict:
        """
        Get statistics about the current queue state

        Returns:
            Dict with queue statistics
        """
        stats = {
            'total_messages': len(self.priority_queue),
            'history_size': len(self.history.get_all()),
            'priority_breakdown': {}
        }

        # Count messages by priority
        for priority, counter, msg in self.priority_queue:
            priority_name = self.get_priority_name(priority)
            stats['priority_breakdown'][priority_name] = \
                stats['priority_breakdown'].get(priority_name, 0) + 1

        return stats

    def auto_detect_priority(self, message: str) -> int:
        """
        Automatically detect message priority based on content

        Args:
            message: The message text to analyze

        Returns:
            Priority level (1-4)
        """
        message_lower = message.lower().strip()

        # Check for urgent keywords
        if any(keyword in message_lower for keyword in self.URGENT_KEYWORDS):
            return 1  # URGENT

        # Check for high priority keywords
        if any(keyword in message_lower for keyword in self.HIGH_KEYWORDS):
            return 2  # HIGH

        # Check for @mentions (indicates direct communication)
        if '@' in message and len(message) > 1:
            return 2  # HIGH

        # Check for ALL CAPS (might indicate urgency)
        if len(message) > 5 and message.isupper():
            return 2  # HIGH

        # Check for multiple exclamation marks
        if message.count('!') >= 3:
            return 2  # HIGH

        # Default to normal priority
        return 3  # NORMAL

    def get_priority_name(self, priority: int) -> str:
        """
        Convert priority number to human-readable name

        Args:
            priority: Priority number (1-4)

        Returns:
            Priority name string
        """
        return self.PRIORITY_NAMES.get(priority, "NORMAL")

    def clear_queue(self) -> int:
        """
        Clear all messages from the priority queue (keeps history)

        Returns:
            Number of messages that were cleared
        """
        cleared_count = len(self.priority_queue)
        self.priority_queue.clear()
        print(f"🧹 Cleared {cleared_count} messages from priority queue")
        return cleared_count

    def clear_all(self) -> Dict:
        """
        Clear both queue and history

        Returns:
            Dict with counts of cleared items
        """
        queue_count = len(self.priority_queue)
        history_count = len(self.history.get_all())

        self.priority_queue.clear()
        self.history.clear()
        self.message_counter = 0

        print(f"🧹 Cleared everything: {queue_count} queue, {history_count} history")
        return {
            'queue_cleared': queue_count,
            'history_cleared': history_count
        }
