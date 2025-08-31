"""
Retry Queue Implementation
Handles failed message delivery with exponential backoff using Deque
"""

import time
import threading
import math
from collections import deque
from typing import Dict, List, Optional, Callable
from datetime import datetime
import json


class RetryQueue:
    """
    Deque-based retry queue that handles failed message delivery with exponential backoff
    """
    
    def __init__(self,
                 max_retries: int = 5,
                 initial_delay: float = 1.0,
                 max_delay: float = 60.0,
                 backoff_multiplier: float = 2.0,
                 success_callback: Optional[Callable] = None,
                 failure_callback: Optional[Callable] = None):
        """
        Initialize retry queue
        
        Args:
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay between retries (seconds)
            max_delay: Maximum delay between retries (seconds)
            backoff_multiplier: Exponential backoff multiplier
            success_callback: Function called when message is successfully retried
            failure_callback: Function called when message fails permanently
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_multiplier = backoff_multiplier
        self.success_callback = success_callback
        self.failure_callback = failure_callback
        
        # Deque for pending retries (FIFO for fairness)
        self.retry_deque = deque()
        
        # Deque for messages currently waiting for retry time
        self.waiting_deque = deque()
        
        # Statistics
        self.stats = {
            'total_messages_added': 0,
            'total_retries_attempted': 0,
            'total_messages_succeeded': 0,
            'total_messages_failed': 0,
            'average_retry_count': 0.0,
            'total_retry_time': 0.0,
            'average_retry_time': 0.0,
            'current_queue_size': 0,
            'current_waiting_size': 0
        }
        
        # Thread control
        self.running = True
        self.lock = threading.Lock()
        
        # Start retry processor thread
        self.processor_thread = threading.Thread(target=self._retry_processor, daemon=True)
        self.processor_thread.start()
        
        print(f"✅ RetryQueue initialized: max_retries={max_retries}, initial_delay={initial_delay}s")
    
    def add_failed_message(self, 
                          message: Dict, 
                          error_reason: str = "unknown_error",
                          original_timestamp: Optional[float] = None) -> bool:
        """
        Add a failed message to the retry queue
        
        Args:
            message: Original message that failed
            error_reason: Reason for failure
            original_timestamp: Original message timestamp
            
        Returns:
            bool: True if message was added to retry queue
        """
        try:
            with self.lock:
                retry_entry = {
                    'message': message.copy(),
                    'retry_count': 0,
                    'max_retries': self.max_retries,
                    'error_reason': error_reason,
                    'original_timestamp': original_timestamp or time.time(),
                    'added_to_retry': time.time(),
                    'next_retry_time': time.time() + self.initial_delay,
                    'retry_history': [],
                    'retry_id': f"retry_{int(time.time() * 1000)}_{len(self.retry_deque)}"
                }
                
                # Add to waiting deque (will be moved to retry deque when ready)
                self.waiting_deque.append(retry_entry)
                self.stats['total_messages_added'] += 1
                self.stats['current_waiting_size'] = len(self.waiting_deque)
                
                print(f"🔄 Added failed message to retry queue: '{message.get('text', '')[:30]}...' (Reason: {error_reason})")
                
                return True
                
        except Exception as e:
            print(f"❌ Error adding message to retry queue: {e}")
            return False
    
    def _retry_processor(self):
        """
        Background thread that processes retry attempts
        """
        print("🔄 Retry processor thread started")
        
        while self.running:
            try:
                current_time = time.time()
                
                with self.lock:
                    # Move messages from waiting to retry queue if ready
                    ready_messages = []
                    remaining_waiting = deque()
                    
                    while self.waiting_deque:
                        entry = self.waiting_deque.popleft()
                        if current_time >= entry['next_retry_time']:
                            ready_messages.append(entry)
                        else:
                            remaining_waiting.append(entry)
                    
                    self.waiting_deque = remaining_waiting
                    
                    # Add ready messages to retry queue
                    for entry in ready_messages:
                        self.retry_deque.append(entry)
                    
                    self.stats['current_queue_size'] = len(self.retry_deque)
                    self.stats['current_waiting_size'] = len(self.waiting_deque)
                
                # Process retry queue
                if self.retry_deque:
                    with self.lock:
                        if self.retry_deque:
                            entry = self.retry_deque.popleft()
                    
                    self._attempt_retry(entry)
                
                time.sleep(0.1)  # Small delay to prevent excessive CPU usage
                
            except Exception as e:
                print(f"❌ Retry processor error: {e}")
                time.sleep(0.5)
    
    def _attempt_retry(self, retry_entry: Dict):
        """
        Attempt to retry a failed message
        
        Args:
            retry_entry: Retry entry with message and metadata
        """
        try:
            retry_entry['retry_count'] += 1
            retry_start_time = time.time()
            
            # Record retry attempt
            retry_attempt = {
                'attempt_number': retry_entry['retry_count'],
                'timestamp': retry_start_time,
                'delay_used': retry_start_time - retry_entry.get('last_attempt', retry_entry['added_to_retry'])
            }
            
            retry_entry['retry_history'].append(retry_attempt)
            retry_entry['last_attempt'] = retry_start_time
            
            self.stats['total_retries_attempted'] += 1
            
            print(f"🔄 RETRY ATTEMPT {retry_entry['retry_count']}/{retry_entry['max_retries']}: {retry_entry['retry_id']}")
            
            # Simulate retry attempt (replace with actual retry logic)
            retry_success = self._simulate_message_delivery(retry_entry['message'])
            
            if retry_success:
                # Success
                retry_time = time.time() - retry_entry['added_to_retry']
                self.stats['total_messages_succeeded'] += 1
                self.stats['total_retry_time'] += retry_time
                
                if self.stats['total_messages_succeeded'] > 0:
                    self.stats['average_retry_time'] = self.stats['total_retry_time'] / self.stats['total_messages_succeeded']
                
                print(f"✅ RETRY SUCCESS: {retry_entry['retry_id']} after {retry_entry['retry_count']} attempts")
                
                if self.success_callback:
                    self.success_callback(retry_entry, retry_time)
                    
            else:
                # Failed - check if should retry again
                if retry_entry['retry_count'] >= retry_entry['max_retries']:
                    # Permanent failure
                    self.stats['total_messages_failed'] += 1
                    
                    print(f"❌ PERMANENT FAILURE: {retry_entry['retry_id']} after {retry_entry['retry_count']} attempts")
                    
                    if self.failure_callback:
                        self.failure_callback(retry_entry, "max_retries_exceeded")
                        
                else:
                    # Schedule for next retry with exponential backoff
                    delay = self._calculate_backoff_delay(retry_entry['retry_count'])
                    retry_entry['next_retry_time'] = time.time() + delay
                    
                    with self.lock:
                        self.waiting_deque.append(retry_entry)
                        self.stats['current_waiting_size'] = len(self.waiting_deque)
                    
                    print(f"⏳ RETRY SCHEDULED: {retry_entry['retry_id']} in {delay:.2f}s (attempt {retry_entry['retry_count'] + 1})")
            
            # Update average retry count
            if self.stats['total_messages_succeeded'] + self.stats['total_messages_failed'] > 0:
                total_finished = self.stats['total_messages_succeeded'] + self.stats['total_messages_failed']
                self.stats['average_retry_count'] = self.stats['total_retries_attempted'] / total_finished
                
        except Exception as e:
            print(f"❌ Retry attempt error: {e}")
    
    def _calculate_backoff_delay(self, retry_count: int) -> float:
        """
        Calculate exponential backoff delay
        
        Args:
            retry_count: Current retry count
            
        Returns:
            float: Delay in seconds
        """
        delay = self.initial_delay * (self.backoff_multiplier ** (retry_count - 1))
        return min(delay, self.max_delay)
    
    def _simulate_message_delivery(self, message: Dict) -> bool:
        """
        Simulate message delivery attempt (replace with actual delivery logic)
        
        Args:
            message: Message to deliver
            
        Returns:
            bool: True if delivery successful, False otherwise
        """
        # For testing purposes, simulate random success/failure
        import random
        
        # Higher chance of success for urgent messages
        if message.get('priority', 3) == 1:
            success_rate = 0.8
        elif message.get('priority', 3) == 2:
            success_rate = 0.7
        else:
            success_rate = 0.6
        
        # Add random delay to simulate network operation
        time.sleep(random.uniform(0.1, 0.3))
        
        return random.random() < success_rate
    
    def add_message_for_delivery(self, message: Dict) -> bool:
        """
        Attempt to deliver a message, adding to retry queue if failed
        
        Args:
            message: Message to deliver
            
        Returns:
            bool: True if delivered successfully, False if added to retry queue
        """
        try:
            # Attempt immediate delivery
            if self._simulate_message_delivery(message):
                print(f"✅ Message delivered successfully: '{message.get('text', '')[:30]}...'")
                return True
            else:
                # Delivery failed, add to retry queue
                self.add_failed_message(message, "delivery_failed")
                return False
                
        except Exception as e:
            print(f"❌ Delivery attempt error: {e}")
            self.add_failed_message(message, f"delivery_error: {str(e)}")
            return False
    
    def get_queue_status(self) -> Dict:
        """
        Get current retry queue status and statistics
        
        Returns:
            Dict: Current status information
        """
        with self.lock:
            return {
                'retry_queue_size': len(self.retry_deque),
                'waiting_queue_size': len(self.waiting_deque),
                'total_pending': len(self.retry_deque) + len(self.waiting_deque),
                'is_processing': self.running,
                'configuration': {
                    'max_retries': self.max_retries,
                    'initial_delay': self.initial_delay,
                    'max_delay': self.max_delay,
                    'backoff_multiplier': self.backoff_multiplier
                },
                'statistics': self.stats.copy()
            }
    
    def get_pending_retries(self) -> List[Dict]:
        """
        Get all pending retry entries
        
        Returns:
            List[Dict]: All pending retry entries with metadata
        """
        with self.lock:
            pending = []
            
            # Add entries from retry queue
            for entry in self.retry_deque:
                pending.append({
                    'retry_id': entry['retry_id'],
                    'message_text': entry['message'].get('text', '')[:50],
                    'retry_count': entry['retry_count'],
                    'status': 'ready_for_retry',
                    'error_reason': entry['error_reason'],
                    'time_in_queue': time.time() - entry['added_to_retry']
                })
            
            # Add entries from waiting queue
            for entry in self.waiting_deque:
                time_until_retry = max(0, entry['next_retry_time'] - time.time())
                pending.append({
                    'retry_id': entry['retry_id'],
                    'message_text': entry['message'].get('text', '')[:50],
                    'retry_count': entry['retry_count'],
                    'status': 'waiting',
                    'error_reason': entry['error_reason'],
                    'time_until_retry': time_until_retry,
                    'time_in_queue': time.time() - entry['added_to_retry']
                })
            
            return pending
    
    def clear_queue(self, clear_waiting: bool = True) -> int:
        """
        Clear all messages from retry queues
        
        Args:
            clear_waiting: Whether to also clear waiting queue
            
        Returns:
            int: Number of messages cleared
        """
        with self.lock:
            cleared_count = len(self.retry_deque)
            
            if clear_waiting:
                cleared_count += len(self.waiting_deque)
                self.waiting_deque.clear()
            
            self.retry_deque.clear()
            
            self.stats['current_queue_size'] = len(self.retry_deque)
            self.stats['current_waiting_size'] = len(self.waiting_deque)
        
        print(f"🗑️ Cleared {cleared_count} messages from retry queue")
        return cleared_count
    
    def force_retry_all(self) -> int:
        """
        Force immediate retry of all waiting messages
        
        Returns:
            int: Number of messages moved to retry queue
        """
        with self.lock:
            moved_count = 0
            
            while self.waiting_deque:
                entry = self.waiting_deque.popleft()
                entry['next_retry_time'] = time.time()  # Set to immediate retry
                self.retry_deque.append(entry)
                moved_count += 1
            
            self.stats['current_queue_size'] = len(self.retry_deque)
            self.stats['current_waiting_size'] = len(self.waiting_deque)
        
        print(f"🚀 Forced retry for {moved_count} messages")
        return moved_count
    
    def update_config(self,
                     max_retries: Optional[int] = None,
                     initial_delay: Optional[float] = None,
                     max_delay: Optional[float] = None,
                     backoff_multiplier: Optional[float] = None):
        """
        Update retry queue configuration
        
        Args:
            max_retries: New maximum retry count
            initial_delay: New initial delay
            max_delay: New maximum delay
            backoff_multiplier: New backoff multiplier
        """
        with self.lock:
            if max_retries is not None:
                self.max_retries = max(1, max_retries)
            if initial_delay is not None:
                self.initial_delay = max(0.1, initial_delay)
            if max_delay is not None:
                self.max_delay = max(self.initial_delay, max_delay)
            if backoff_multiplier is not None:
                self.backoff_multiplier = max(1.0, backoff_multiplier)
        
        print(f"⚙️ Retry config updated: max_retries={self.max_retries}, initial_delay={self.initial_delay}s")
    
    def stop(self):
        """Stop the retry processor"""
        self.running = False
        
        # Wait for thread to finish
        if self.processor_thread.is_alive():
            self.processor_thread.join(timeout=2.0)
        
        print(f"⏹️ Retry queue stopped. Final stats: {self.stats}")
    
    def export_retry_log(self) -> Dict:
        """
        Export detailed retry log for analysis
        
        Returns:
            Dict: Comprehensive retry processing data
        """
        with self.lock:
            return {
                'timestamp': datetime.now().isoformat(),
                'configuration': {
                    'max_retries': self.max_retries,
                    'initial_delay': self.initial_delay,
                    'max_delay': self.max_delay,
                    'backoff_multiplier': self.backoff_multiplier
                },
                'current_state': {
                    'retry_queue_size': len(self.retry_deque),
                    'waiting_queue_size': len(self.waiting_deque),
                    'is_running': self.running
                },
                'statistics': self.stats.copy(),
                'pending_retries': self.get_pending_retries()
            }


# Example callback functions
def retry_success_callback(retry_entry: Dict, total_retry_time: float):
    """
    Example callback for successful retry
    
    Args:
        retry_entry: The retry entry that succeeded
        total_retry_time: Total time spent retrying
    """
    print(f"🎉 RETRY SUCCESS CALLBACK: {retry_entry['retry_id']}")
    print(f"   📊 Attempts: {retry_entry['retry_count']}")
    print(f"   ⏱️ Total time: {total_retry_time:.2f}s")


def retry_failure_callback(retry_entry: Dict, failure_reason: str):
    """
    Example callback for permanent retry failure
    
    Args:
        retry_entry: The retry entry that failed permanently
        failure_reason: Reason for permanent failure
    """
    print(f"💔 RETRY FAILURE CALLBACK: {retry_entry['retry_id']}")
    print(f"   📊 Final attempt count: {retry_entry['retry_count']}")
    print(f"   ❌ Failure reason: {failure_reason}")
    print(f"   📝 Original message: '{retry_entry['message'].get('text', '')[:50]}...'")


# Example usage and testing
if __name__ == "__main__":
    # Create retry queue with callbacks
    retry_queue = RetryQueue(
        max_retries=3,
        initial_delay=0.5,
        max_delay=10.0,
        backoff_multiplier=2.0,
        success_callback=retry_success_callback,
        failure_callback=retry_failure_callback
    )
    
    # Test with sample failed messages
    sample_failed_messages = [
        {"user": "Alice", "text": "Failed message 1", "priority": 1, "id": "msg_1"},
        {"user": "Bob", "text": "Failed message 2", "priority": 2, "id": "msg_2"},
        {"user": "Charlie", "text": "Failed message 3", "priority": 3, "id": "msg_3"},
    ]
    
    print("\n🧪 Testing retry queue...")
    
    # Add failed messages
    for msg in sample_failed_messages:
        retry_queue.add_failed_message(msg, "network_timeout")
    
    # Test direct delivery attempts
    print("\n🚀 Testing direct delivery attempts...")
    test_messages = [
        {"user": "David", "text": "Test delivery 1", "priority": 2},
        {"user": "Eve", "text": "Test delivery 2", "priority": 1},
    ]
    
    for msg in test_messages:
        success = retry_queue.add_message_for_delivery(msg)
        print(f"   {'✅' if success else '❌'} Message: '{msg['text']}'")
    
    # Wait for some processing
    time.sleep(5)
    
    # Check status
    status = retry_queue.get_queue_status()
    print(f"\n📊 Status: {json.dumps(status, indent=2)}")
    
    # Show pending retries
    pending = retry_queue.get_pending_retries()
    if pending:
        print(f"\n📋 Pending Retries: {len(pending)}")
        for retry in pending:
            print(f"   🔄 {retry['retry_id']}: {retry['message_text']} (attempt {retry['retry_count']})")
    
    # Wait a bit more for retries
    time.sleep(3)
    
    # Clean shutdown
    retry_queue.stop()