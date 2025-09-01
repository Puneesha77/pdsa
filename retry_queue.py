"""
Retry Queue with Exponential Backoff
Handles failed message delivery attempts with intelligent retry logic
"""

import time
import threading
import math
from typing import Dict, List, Callable, Optional, Deque
from collections import deque
from enum import Enum


class RetryStatus(Enum):
    """Status of retry attempts"""
    PENDING = "pending"
    RETRYING = "retrying" 
    SUCCESS = "success"
    FAILED = "failed"
    ABANDONED = "abandoned"


class RetryMessage:
    """
    Wrapper for messages in the retry queue
    """
    def __init__(self, message: Dict, max_retries: int = 5):
        self.message = message
        self.max_retries = max_retries
        self.attempt_count = 0
        self.status = RetryStatus.PENDING
        self.first_attempt_time = time.time()
        self.last_attempt_time = None
        self.next_retry_time = None
        self.failure_reasons = []
        
    def calculate_next_retry(self, base_delay: float = 1.0, max_delay: float = 60.0):
        """Calculate next retry time using exponential backoff"""
        # Exponential backoff: base_delay * (2 ^ attempt_count) + jitter
        import random
        delay = base_delay * (2 ** self.attempt_count)
        delay = min(delay, max_delay)  # Cap at max_delay
        jitter = random.uniform(0.1, 0.5) * delay  # Add 10-50% jitter
        
        self.next_retry_time = time.time() + delay + jitter
        return self.next_retry_time
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for statistics"""
        return {
            'message_id': id(self.message),
            'status': self.status.value,
            'attempt_count': self.attempt_count,
            'max_retries': self.max_retries,
            'first_attempt_time': self.first_attempt_time,
            'last_attempt_time': self.last_attempt_time,
            'next_retry_time': self.next_retry_time,
            'failure_reasons': self.failure_reasons.copy(),
            'total_time_in_queue': time.time() - self.first_attempt_time
        }


class RetryQueue:
    """
    A deque-based retry queue with exponential backoff for failed message delivery.
    
    Features:
    - Double-ended queue (deque) for efficient add/remove operations
    - Exponential backoff with jitter to prevent thundering herd
    - Configurable max retry attempts and delays
    - Automatic retry scheduling with background worker
    - Comprehensive failure tracking and statistics
    - Thread-safe operations
    """

    def __init__(self, 
                 max_retries: int = 5,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 retry_callback: Optional[Callable] = None,
                 cleanup_interval: float = 300.0):  # 5 minutes
        """
        Initialize the retry queue

        Args:
            max_retries: Maximum number of retry attempts per message
            base_delay: Base delay in seconds for exponential backoff
            max_delay: Maximum delay between retries
            retry_callback: Function to call when retrying message delivery
            cleanup_interval: How often to clean up old completed messages
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retry_callback = retry_callback
        self.cleanup_interval = cleanup_interval
        
        # Deque for efficient operations at both ends
        self.retry_queue: Deque[RetryMessage] = deque()
        self.completed_messages: Deque[RetryMessage] = deque(maxlen=1000)  # Keep history
        
        # Thread synchronization
        self.lock = threading.RLock()
        self.worker_thread = None
        self.stop_worker = threading.Event()
        
        # Statistics
        self.stats = {
            'total_messages_added': 0,
            'total_retry_attempts': 0,
            'successful_retries': 0,
            'failed_messages': 0,
            'abandoned_messages': 0,
            'current_queue_size': 0,
            'average_retry_count': 0.0,
            'longest_retry_time': 0.0,
            'retry_success_rate': 0.0,
            'messages_by_status': {status.value: 0 for status in RetryStatus}
        }
        
        # Start background worker
        self._start_worker()
        
        print(f"🔄 Retry Queue initialized: max_retries={max_retries}, base_delay={base_delay}s, max_delay={max_delay}s")

    def enqueue(self, message: Dict, failure_reason: str = "Initial failure") -> bool:
        """
        Add a failed message to the retry queue

        Args:
            message: Message object that failed to deliver
            failure_reason: Reason why the message failed

        Returns:
            True if message was added successfully
        """
        try:
            with self.lock:
                retry_msg = RetryMessage(message, self.max_retries)
                retry_msg.failure_reasons.append(f"Initial: {failure_reason}")
                retry_msg.calculate_next_retry(self.base_delay, self.max_delay)
                
                # Add to front of deque (LIFO for failed messages - retry recent failures first)
                self.retry_queue.appendleft(retry_msg)
                
                self.stats['total_messages_added'] += 1
                self.stats['current_queue_size'] = len(self.retry_queue)
                self.stats['messages_by_status'][RetryStatus.PENDING.value] += 1
                
                print(f"🔄 Added message to retry queue (queue size: {len(self.retry_queue)})")
                print(f"⏰ Next retry scheduled for: {time.ctime(retry_msg.next_retry_time)}")
                
                return True
                
        except Exception as e:
            print(f"🚨 Error adding message to retry queue: {str(e)}")
            return False

    def add_retry_failure(self, message: Dict, failure_reason: str) -> bool:
        """
        Add a message that failed during retry attempt

        Args:
            message: Message object that failed retry
            failure_reason: Specific reason for retry failure

        Returns:
            True if message was updated successfully
        """
        try:
            with self.lock:
                # Find the message in the retry queue
                for retry_msg in self.retry_queue:
                    if id(retry_msg.message) == id(message):
                        retry_msg.attempt_count += 1
                        retry_msg.last_attempt_time = time.time()
                        retry_msg.failure_reasons.append(f"Attempt {retry_msg.attempt_count}: {failure_reason}")
                        
                        if retry_msg.attempt_count >= self.max_retries:
                            # Max retries reached - abandon message
                            retry_msg.status = RetryStatus.ABANDONED
                            self._move_to_completed(retry_msg)
                            print(f"❌ Message abandoned after {retry_msg.attempt_count} attempts")
                        else:
                            # Schedule next retry
                            retry_msg.calculate_next_retry(self.base_delay, self.max_delay)
                            retry_msg.status = RetryStatus.PENDING
                            print(f"🔄 Message retry {retry_msg.attempt_count}/{self.max_retries} failed, next attempt: {time.ctime(retry_msg.next_retry_time)}")
                        
                        self.stats['total_retry_attempts'] += 1
                        return True
                
                print(f"⚠️ Message not found in retry queue for failure update")
                return False
                
        except Exception as e:
            print(f"🚨 Error updating retry failure: {str(e)}")
            return False

    def mark_retry_success(self, message: Dict) -> bool:
        """
        Mark a message as successfully delivered after retry

        Args:
            message: Message that was successfully delivered

        Returns:
            True if message was found and marked successful
        """
        try:
            with self.lock:
                for retry_msg in self.retry_queue:
                    if id(retry_msg.message) == id(message):
                        retry_msg.status = RetryStatus.SUCCESS
                        retry_msg.last_attempt_time = time.time()
                        
                        self._move_to_completed(retry_msg)
                        
                        self.stats['successful_retries'] += 1
                        print(f"✅ Message successfully delivered after {retry_msg.attempt_count + 1} attempt(s)")
                        return True
                
                return False
                
        except Exception as e:
            print(f"🚨 Error marking retry success: {str(e)}")
            return False

    def get_ready_messages(self) -> List[RetryMessage]:
        """
        Get messages that are ready for retry (past their next_retry_time)

        Returns:
            List of RetryMessage objects ready for retry
        """
        with self.lock:
            current_time = time.time()
            ready_messages = []
            
            # Check messages from the back of deque (oldest failures first for ready messages)
            temp_deque = deque()
            
            while self.retry_queue:
                retry_msg = self.retry_queue.pop()
                if (retry_msg.status == RetryStatus.PENDING and 
                    retry_msg.next_retry_time and 
                    current_time >= retry_msg.next_retry_time):
                    
                    retry_msg.status = RetryStatus.RETRYING
                    ready_messages.append(retry_msg)
                else:
                    temp_deque.appendleft(retry_msg)
            
            # Restore non-ready messages to queue
            self.retry_queue.extend(temp_deque)
            
            return ready_messages

    def _move_to_completed(self, retry_msg: RetryMessage):
        """Move a completed message to the completed messages deque"""
        # Remove from active queue
        try:
            self.retry_queue.remove(retry_msg)
        except ValueError:
            pass  # Already removed
        
        # Add to completed (with size limit)
        self.completed_messages.append(retry_msg)
        
        # Update statistics
        self.stats['current_queue_size'] = len(self.retry_queue)
        self.stats['messages_by_status'][retry_msg.status.value] += 1

    def _start_worker(self):
        """Start the background worker thread"""
        if self.worker_thread and self.worker_thread.is_alive():
            return
        
        self.stop_worker.clear()
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        print("🔧 Retry queue background worker started")

    def _worker_loop(self):
        """Background worker that processes retry attempts"""
        while not self.stop_worker.is_set():
            try:
                # Get messages ready for retry
                ready_messages = self.get_ready_messages()
                
                for retry_msg in ready_messages:
                    if self.retry_callback:
                        try:
                            print(f"🔄 Attempting retry {retry_msg.attempt_count + 1}/{self.max_retries} for message")
                            success = self.retry_callback(retry_msg.message, retry_msg.attempt_count + 1)
                            
                            if success:
                                self.mark_retry_success(retry_msg.message)
                            else:
                                self.add_retry_failure(retry_msg.message, "Callback returned False")
                        
                        except Exception as e:
                            self.add_retry_failure(retry_msg.message, f"Callback exception: {str(e)}")
                    
                    # Small delay between retry attempts
                    time.sleep(0.1)
                
                # Clean up old completed messages periodically
                self._cleanup_old_messages()
                
                # Sleep before next check
                time.sleep(1.0)
                
            except Exception as e:
                print(f"🚨 Error in retry worker loop: {str(e)}")
                time.sleep(1.0)

    def _cleanup_old_messages(self):
        """Clean up very old completed messages"""
        current_time = time.time()
        cleanup_threshold = current_time - self.cleanup_interval
        
        with self.lock:
            # The deque automatically limits size, but we can clean up by status too
            while (self.completed_messages and 
                   self.completed_messages[0].first_attempt_time < cleanup_threshold):
                self.completed_messages.popleft()

    def get_stats(self) -> Dict:
        """
        Get comprehensive retry queue statistics

        Returns:
            Dictionary with detailed retry statistics
        """
        with self.lock:
            # Update current statistics
            self.stats['current_queue_size'] = len(self.retry_queue)
            
            # Calculate average retry count
            completed_count = len(self.completed_messages)
            if completed_count > 0:
                total_attempts = sum(msg.attempt_count for msg in self.completed_messages)
                self.stats['average_retry_count'] = round(total_attempts / completed_count, 2)
                
                # Calculate success rate
                successful = sum(1 for msg in self.completed_messages if msg.status == RetryStatus.SUCCESS)
                self.stats['retry_success_rate'] = round((successful / completed_count) * 100, 2)
                
                # Find longest retry time
                longest_time = max(msg.total_time_in_queue for msg in self.completed_messages)
                self.stats['longest_retry_time'] = round(longest_time, 2)
            
            return self.stats.copy()

    def get_queue_contents(self) -> List[Dict]:
        """
        Get information about all messages currently in the retry queue

        Returns:
            List of message information dictionaries
        """
        with self.lock:
            return [retry_msg.to_dict() for retry_msg in self.retry_queue]

    def force_retry_all(self) -> int:
        """
        Force retry all pending messages immediately (ignore timing)

        Returns:
            Number of messages that were forced to retry
        """
        with self.lock:
            count = 0
            current_time = time.time()
            
            for retry_msg in self.retry_queue:
                if retry_msg.status == RetryStatus.PENDING:
                    retry_msg.next_retry_time = current_time
                    count += 1
            
            print(f"🔨 Forced {count} messages to retry immediately")
            return count

    def clear(self) -> Dict:
        """
        Clear all messages from the retry queue

        Returns:
            Dictionary with counts of cleared messages by status
        """
        with self.lock:
            cleared_counts = {}
            
            for retry_msg in self.retry_queue:
                status = retry_msg.status.value
                cleared_counts[status] = cleared_counts.get(status, 0) + 1
            
            total_cleared = len(self.retry_queue)
            self.retry_queue.clear()
            self.stats['current_queue_size'] = 0
            
            print(f"🧹 Cleared {total_cleared} messages from retry queue")
            return cleared_counts

    def stop(self):
        """Stop the background worker thread"""
        self.stop_worker.set()
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5.0)
        print("🛑 Retry queue worker stopped")

    def set_retry_callback(self, callback: Callable):
        """
        Set or update the retry callback function

        Args:
            callback: Function to call when retrying message delivery
        """
        self.retry_callback = callback
        print("🔄 Retry callback updated")

    def __del__(self):
        """Cleanup when object is destroyed"""
        self.stop()