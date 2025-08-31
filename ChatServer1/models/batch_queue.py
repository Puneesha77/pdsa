"""
Batch Queue Implementation
Groups messages for efficient network transmission
"""

import time
import threading
from collections import deque
from typing import List, Dict, Optional, Callable
from datetime import datetime
import json


class BatchQueue:
    """
    FIFO-based batch queue that groups messages for efficient network transmission
    """
    
    def __init__(self, 
                 min_batch_size: int = 5, 
                 max_batch_size: int = 50, 
                 max_wait_time: float = 2.0,
                 callback: Optional[Callable] = None):
        """
        Initialize batch queue
        
        Args:
            min_batch_size: Minimum messages before sending batch
            max_batch_size: Maximum messages in a single batch
            max_wait_time: Maximum time to wait before sending incomplete batch (seconds)
            callback: Function to call when batch is ready for transmission
        """
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.max_wait_time = max_wait_time
        self.callback = callback
        
        # Main message queue (FIFO)
        self.message_queue = deque()
        
        # Current batch being assembled
        self.current_batch = []
        self.batch_start_time = None
        
        # Statistics
        self.stats = {
            'total_messages_processed': 0,
            'total_batches_sent': 0,
            'average_batch_size': 0.0,
            'total_wait_time': 0.0,
            'average_wait_time': 0.0,
            'efficiency_score': 0.0
        }
        
        # Thread control
        self.running = True
        self.lock = threading.Lock()
        
        # Start batch processor thread
        self.processor_thread = threading.Thread(target=self._batch_processor, daemon=True)
        self.processor_thread.start()
        
        print(f"✅ BatchQueue initialized: min={min_batch_size}, max={max_batch_size}, wait={max_wait_time}s")
    
    def add_message(self, message: Dict) -> bool:
        """
        Add message to batch queue
        
        Args:
            message: Message dictionary with required fields
            
        Returns:
            bool: True if message was added successfully
        """
        try:
            with self.lock:
                # Add timestamp if not present
                if 'batch_timestamp' not in message:
                    message['batch_timestamp'] = time.time()
                
                # Add unique batch ID
                message['batch_id'] = f"batch_{int(time.time() * 1000)}_{len(self.message_queue)}"
                
                # Add to queue
                self.message_queue.append(message)
                self.stats['total_messages_processed'] += 1
                
                print(f"📥 Added message to batch queue: '{message.get('text', '')[:30]}...' (Queue size: {len(self.message_queue)})")
                
                return True
                
        except Exception as e:
            print(f"❌ Error adding message to batch queue: {e}")
            return False
    
    def _batch_processor(self):
        """
        Background thread that processes messages into batches
        """
        print("🔄 Batch processor thread started")
        
        while self.running:
            try:
                with self.lock:
                    # Check if we have messages to process
                    if not self.message_queue:
                        time.sleep(0.1)
                        continue
                    
                    # Initialize batch if empty
                    if not self.current_batch:
                        self.batch_start_time = time.time()
                    
                    # Move messages from queue to current batch
                    while self.message_queue and len(self.current_batch) < self.max_batch_size:
                        message = self.message_queue.popleft()
                        self.current_batch.append(message)
                    
                    # Check if batch should be sent
                    should_send = False
                    send_reason = ""
                    
                    if len(self.current_batch) >= self.max_batch_size:
                        should_send = True
                        send_reason = "max_size_reached"
                    elif len(self.current_batch) >= self.min_batch_size:
                        time_elapsed = time.time() - self.batch_start_time
                        if time_elapsed >= self.max_wait_time:
                            should_send = True
                            send_reason = "max_wait_time_reached"
                    elif self.current_batch and self.batch_start_time:
                        time_elapsed = time.time() - self.batch_start_time
                        if time_elapsed >= self.max_wait_time * 2:  # Extended wait for small batches
                            should_send = True
                            send_reason = "extended_wait_timeout"
                
                if should_send and self.current_batch:
                    self._send_batch(send_reason)
                
                time.sleep(0.1)  # Small delay to prevent excessive CPU usage
                
            except Exception as e:
                print(f"❌ Batch processor error: {e}")
                time.sleep(0.5)
    
    def _send_batch(self, reason: str):
        """
        Send current batch and reset for next batch
        
        Args:
            reason: Reason why batch is being sent
        """
        if not self.current_batch:
            return
        
        batch_data = {
            'batch_id': f"batch_{int(time.time() * 1000)}",
            'messages': self.current_batch.copy(),
            'batch_size': len(self.current_batch),
            'send_reason': reason,
            'timestamp': time.time(),
            'wait_time': time.time() - self.batch_start_time if self.batch_start_time else 0
        }
        
        # Update statistics
        self.stats['total_batches_sent'] += 1
        self.stats['total_wait_time'] += batch_data['wait_time']
        self.stats['average_wait_time'] = self.stats['total_wait_time'] / self.stats['total_batches_sent']
        
        # Calculate average batch size
        total_messages_in_batches = self.stats['total_batches_sent'] * batch_data['batch_size']
        self.stats['average_batch_size'] = total_messages_in_batches / self.stats['total_batches_sent']
        
        # Calculate efficiency score (higher is better)
        self.stats['efficiency_score'] = min(100, (batch_data['batch_size'] / self.max_batch_size) * 100)
        
        print(f"📦 BATCH SENT: {batch_data['batch_size']} messages, reason: {reason}, wait: {batch_data['wait_time']:.2f}s")
        
        # Call callback if provided
        if self.callback:
            try:
                self.callback(batch_data)
            except Exception as e:
                print(f"❌ Batch callback error: {e}")
        
        # Reset current batch
        self.current_batch = []
        self.batch_start_time = None
    
    def force_send_batch(self) -> Optional[Dict]:
        """
        Force send current batch regardless of size or timing
        
        Returns:
            Dict: Batch data that was sent, or None if no messages
        """
        with self.lock:
            if self.current_batch:
                batch_data = {
                    'batch_id': f"forced_batch_{int(time.time() * 1000)}",
                    'messages': self.current_batch.copy(),
                    'batch_size': len(self.current_batch),
                    'send_reason': 'forced',
                    'timestamp': time.time(),
                    'wait_time': time.time() - self.batch_start_time if self.batch_start_time else 0
                }
                
                print(f"🚀 FORCED BATCH SEND: {batch_data['batch_size']} messages")
                
                if self.callback:
                    self.callback(batch_data)
                
                self.current_batch = []
                self.batch_start_time = None
                
                return batch_data
        
        return None
    
    def get_queue_status(self) -> Dict:
        """
        Get current queue status and statistics
        
        Returns:
            Dict: Current status information
        """
        with self.lock:
            return {
                'queue_size': len(self.message_queue),
                'current_batch_size': len(self.current_batch),
                'batch_wait_time': time.time() - self.batch_start_time if self.batch_start_time else 0,
                'is_processing': self.running,
                'configuration': {
                    'min_batch_size': self.min_batch_size,
                    'max_batch_size': self.max_batch_size,
                    'max_wait_time': self.max_wait_time
                },
                'statistics': self.stats.copy()
            }
    
    def update_config(self, 
                     min_batch_size: Optional[int] = None,
                     max_batch_size: Optional[int] = None, 
                     max_wait_time: Optional[float] = None):
        """
        Update batch queue configuration
        
        Args:
            min_batch_size: New minimum batch size
            max_batch_size: New maximum batch size  
            max_wait_time: New maximum wait time
        """
        with self.lock:
            if min_batch_size is not None:
                self.min_batch_size = max(1, min_batch_size)
            if max_batch_size is not None:
                self.max_batch_size = max(self.min_batch_size, max_batch_size)
            if max_wait_time is not None:
                self.max_wait_time = max(0.1, max_wait_time)
        
        print(f"⚙️ Batch config updated: min={self.min_batch_size}, max={self.max_batch_size}, wait={self.max_wait_time}s")
    
    def clear_queue(self):
        """Clear all messages from queue and current batch"""
        with self.lock:
            cleared_count = len(self.message_queue) + len(self.current_batch)
            self.message_queue.clear()
            self.current_batch = []
            self.batch_start_time = None
        
        print(f"🗑️ Cleared {cleared_count} messages from batch queue")
        
        return cleared_count
    
    def stop(self):
        """Stop the batch processor"""
        self.running = False
        
        # Send any remaining messages
        self.force_send_batch()
        
        # Wait for thread to finish
        if self.processor_thread.is_alive():
            self.processor_thread.join(timeout=2.0)
        
        print("⏹️ Batch queue stopped")
    
    def get_pending_messages(self) -> List[Dict]:
        """
        Get all pending messages (in queue + current batch)
        
        Returns:
            List[Dict]: All pending messages
        """
        with self.lock:
            return list(self.message_queue) + self.current_batch
    
    def export_batch_log(self) -> Dict:
        """
        Export detailed batch processing log for analysis
        
        Returns:
            Dict: Comprehensive batch processing data
        """
        return {
            'timestamp': datetime.now().isoformat(),
            'configuration': {
                'min_batch_size': self.min_batch_size,
                'max_batch_size': self.max_batch_size,
                'max_wait_time': self.max_wait_time
            },
            'current_state': {
                'queue_size': len(self.message_queue),
                'current_batch_size': len(self.current_batch),
                'is_running': self.running
            },
            'statistics': self.stats.copy()
        }


# Example callback function for testing
def batch_transmission_callback(batch_data: Dict):
    """
    Example callback function for handling batch transmission
    
    Args:
        batch_data: Batch information with messages to transmit
    """
    print(f"🌐 TRANSMITTING BATCH: {batch_data['batch_id']}")
    print(f"   📊 Size: {batch_data['batch_size']} messages")
    print(f"   ⏱️ Wait time: {batch_data['wait_time']:.2f}s")
    print(f"   🎯 Reason: {batch_data['send_reason']}")
    
    # Here you would typically send the batch to clients via SocketIO
    # For example: socketio.emit('batch_messages', batch_data, broadcast=True)


# Example usage and testing
if __name__ == "__main__":
    # Create batch queue with callback
    batch_queue = BatchQueue(
        min_batch_size=3,
        max_batch_size=10,
        max_wait_time=1.5,
        callback=batch_transmission_callback
    )
    
    # Test with sample messages
    sample_messages = [
        {"user": "Alice", "text": "Hello everyone!", "priority": 3},
        {"user": "Bob", "text": "URGENT: Server is down!", "priority": 1},
        {"user": "Charlie", "text": "Meeting at 3pm", "priority": 2},
        {"user": "David", "text": "Thanks for the update", "priority": 3},
        {"user": "Eve", "text": "Can someone help with this bug?", "priority": 2},
    ]
    
    print("\n🧪 Testing batch queue...")
    
    # Add messages with delays to test batching
    for i, msg in enumerate(sample_messages):
        batch_queue.add_message(msg)
        if i < len(sample_messages) - 1:
            time.sleep(0.3)  # Small delay between messages
    
    # Wait for processing
    time.sleep(3)
    
    # Check status
    status = batch_queue.get_queue_status()
    print(f"\n📊 Final Status: {json.dumps(status, indent=2)}")
    
    # Clean shutdown
    batch_queue.stop()