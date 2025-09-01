"""
Batch Queue for Network Efficiency
Groups messages for efficient network transmission
"""

import time
import threading
from typing import Dict, List, Callable, Optional
from collections import deque


class BatchQueue:
    """
    A FIFO batch queue that groups messages for network efficiency.
    
    Features:
    - Groups 5-50 messages per batch for optimal network transmission
    - Configurable batch size and timeout
    - Automatic batch sending when size limit reached or timeout occurs
    - Thread-safe operations
    - Network efficiency statistics tracking
    """

    def __init__(self, 
                 min_batch_size: int = 5,
                 max_batch_size: int = 50, 
                 timeout_seconds: float = 2.0,
                 send_callback: Optional[Callable] = None):
        """
        Initialize the batch queue

        Args:
            min_batch_size: Minimum messages before sending batch
            max_batch_size: Maximum messages per batch
            timeout_seconds: Max time to wait before sending incomplete batch
            send_callback: Function to call when sending batch
        """
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.timeout_seconds = timeout_seconds
        self.send_callback = send_callback
        
        # Thread-safe queue for pending messages
        self.pending_messages = deque()
        self.lock = threading.Lock()
        
        # Timer for batch timeout
        self.batch_timer = None
        
        # Statistics
        self.stats = {
            'total_messages_processed': 0,
            'total_batches_sent': 0,
            'messages_in_current_batch': 0,
            'average_batch_size': 0.0,
            'network_efficiency_percent': 0.0,
            'last_batch_sent_at': None,
            'timeout_triggered_batches': 0,
            'size_triggered_batches': 0
        }
        
        print(f"🔄 Batch Queue initialized: {min_batch_size}-{max_batch_size} messages, {timeout_seconds}s timeout")

    def enqueue(self, message: Dict) -> bool:
        """
        Add a message to the batch queue

        Args:
            message: Message object to add to batch

        Returns:
            True if message was added successfully
        """
        try:
            with self.lock:
                # Add message to pending queue
                message['batch_enqueue_time'] = time.time()
                self.pending_messages.append(message)
                
                self.stats['total_messages_processed'] += 1
                self.stats['messages_in_current_batch'] = len(self.pending_messages)
                
                print(f"📦 Added message to batch queue (size: {len(self.pending_messages)}/{self.max_batch_size})")
                
                # Check if we should send the batch
                if len(self.pending_messages) >= self.max_batch_size:
                    # Send immediately if max size reached
                    self._send_batch_now(trigger='size_limit')
                    return True
                elif len(self.pending_messages) == 1:
                    # Start timer for first message in batch
                    self._start_batch_timer()
                
                return True
                
        except Exception as e:
            print(f"🚨 Error adding message to batch queue: {str(e)}")
            return False

    def _start_batch_timer(self):
        """Start or restart the batch timeout timer"""
        # Cancel existing timer
        if self.batch_timer:
            self.batch_timer.cancel()
        
        # Start new timer
        self.batch_timer = threading.Timer(self.timeout_seconds, self._timeout_batch_send)
        self.batch_timer.start()
        print(f"⏲️ Batch timer started ({self.timeout_seconds}s)")

    def _timeout_batch_send(self):
        """Handle batch timeout - send whatever messages we have"""
        with self.lock:
            if self.pending_messages and len(self.pending_messages) >= self.min_batch_size:
                self._send_batch_now(trigger='timeout')
            elif self.pending_messages:
                print(f"⏳ Batch timeout but only {len(self.pending_messages)} messages (min: {self.min_batch_size})")

    def _send_batch_now(self, trigger: str = 'manual'):
        """
        Send the current batch immediately (must be called with lock held)
        
        Args:
            trigger: What triggered the batch send ('size_limit', 'timeout', 'manual')
        """
        if not self.pending_messages:
            return
        
        # Create batch from pending messages
        batch = list(self.pending_messages)
        batch_size = len(batch)
        
        # Clear pending queue
        self.pending_messages.clear()
        
        # Cancel timer
        if self.batch_timer:
            self.batch_timer.cancel()
            self.batch_timer = None
        
        # Update statistics
        self.stats['total_batches_sent'] += 1
        self.stats['messages_in_current_batch'] = 0
        self.stats['last_batch_sent_at'] = time.time()
        
        if trigger == 'timeout':
            self.stats['timeout_triggered_batches'] += 1
        elif trigger == 'size_limit':
            self.stats['size_triggered_batches'] += 1
        
        # Calculate network efficiency
        self._calculate_efficiency()
        
        print(f"📤 Sending batch: {batch_size} messages (trigger: {trigger})")
        
        # Send batch using callback
        if self.send_callback:
            try:
                self.send_callback(batch, batch_size, trigger)
            except Exception as e:
                print(f"🚨 Error in batch send callback: {str(e)}")

    def force_send_batch(self) -> int:
        """
        Force send the current batch regardless of size/timeout
        
        Returns:
            Number of messages in the batch that was sent
        """
        with self.lock:
            batch_size = len(self.pending_messages)
            if batch_size > 0:
                self._send_batch_now(trigger='forced')
                print(f"🔨 Force-sent batch of {batch_size} messages")
            return batch_size

    def get_stats(self) -> Dict:
        """
        Get current batch queue statistics
        
        Returns:
            Dictionary with batch queue statistics
        """
        with self.lock:
            # Update current batch info
            self.stats['messages_in_current_batch'] = len(self.pending_messages)
            return self.stats.copy()

    def _calculate_efficiency(self):
        """Calculate network efficiency percentage"""
        if self.stats['total_batches_sent'] > 0:
            # Average batch size
            avg_batch = self.stats['total_messages_processed'] / self.stats['total_batches_sent']
            self.stats['average_batch_size'] = round(avg_batch, 2)
            
            # Efficiency: how close we are to max batch size
            efficiency = (avg_batch / self.max_batch_size) * 100
            self.stats['network_efficiency_percent'] = round(efficiency, 2)

    def clear(self) -> int:
        """
        Clear all pending messages from the batch queue
        
        Returns:
            Number of messages that were cleared
        """
        with self.lock:
            count = len(self.pending_messages)
            self.pending_messages.clear()
            
            if self.batch_timer:
                self.batch_timer.cancel()
                self.batch_timer = None
            
            self.stats['messages_in_current_batch'] = 0
            print(f"🧹 Cleared {count} messages from batch queue")
            return count

    def is_empty(self) -> bool:
        """
        Check if the batch queue is empty
        
        Returns:
            True if no pending messages
        """
        with self.lock:
            return len(self.pending_messages) == 0

    def current_batch_size(self) -> int:
        """
        Get the current number of messages in the batch
        
        Returns:
            Number of pending messages
        """
        with self.lock:
            return len(self.pending_messages)

    def set_send_callback(self, callback: Callable):
        """
        Set or update the batch send callback function
        
        Args:
            callback: Function to call when sending batches
        """
        self.send_callback = callback
        print("🔄 Batch send callback updated")