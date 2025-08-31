"""
Offline Queue Implementation
Stores messages for disconnected users and delivers them when they reconnect
"""

import time
import threading
from collections import deque, defaultdict
from typing import Dict, List, Optional, Callable, Set
from datetime import datetime
import json


class OfflineQueue:
    """
    Queue-based system for storing messages for offline users
    
    Features:
    - Stores messages per user in FIFO order
    - Automatic cleanup of old messages
    - Delivery tracking and confirmation
    - Integration with UserManager for online/offline status
    - Thread-safe operations
    """
    
    def __init__(self,
                 max_messages_per_user: int = 100,
                 message_expiry_hours: int = 24,
                 auto_cleanup_interval: int = 300,  # 5 minutes
                 delivery_callback: Optional[Callable] = None):
        """
        Initialize offline queue
        
        Args:
            max_messages_per_user: Maximum messages stored per user
            message_expiry_hours: Hours after which messages expire
            auto_cleanup_interval: Seconds between automatic cleanup runs
            delivery_callback: Function called when messages are delivered to user
        """
        self.max_messages_per_user = max_messages_per_user
        self.message_expiry_seconds = message_expiry_hours * 3600
        self.auto_cleanup_interval = auto_cleanup_interval
        self.delivery_callback = delivery_callback
        
        # User message queues (username -> deque of messages)
        self.user_queues: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.max_messages_per_user))
        
        # Message metadata tracking
        self.message_metadata: Dict[str, Dict] = {}  # message_id -> metadata
        
        # Statistics
        self.stats = {
            'total_messages_stored': 0,
            'total_messages_delivered': 0,
            'total_messages_expired': 0,
            'total_cleanup_runs': 0,
            'users_with_offline_messages': 0,
            'average_messages_per_user': 0.0,
            'oldest_undelivered_age': 0.0,
            'delivery_success_rate': 0.0
        }
        
        # Thread control
        self.running = True
        self.lock = threading.Lock()
        
        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_processor, daemon=True)
        self.cleanup_thread.start()
        
        print(f"✅ OfflineQueue initialized: max_per_user={max_messages_per_user}, expiry={message_expiry_hours}h")
    
    def store_message_for_user(self, username: str, message: Dict) -> bool:
        """
        Store a message for an offline user
        
        Args:
            username: Target username
            message: Message dictionary
            
        Returns:
            bool: True if message was stored successfully
        """
        try:
            with self.lock:
                # Create message with offline queue metadata
                offline_message = {
                    'message_id': f"offline_{username}_{int(time.time() * 1000)}_{len(self.user_queues[username])}",
                    'target_username': username,
                    'stored_timestamp': time.time(),
                    'expiry_timestamp': time.time() + self.message_expiry_seconds,
                    'delivery_attempts': 0,
                    'is_delivered': False,
                    'original_message': message.copy()
                }
                
                # Add to user's queue
                self.user_queues[username].append(offline_message)
                
                # Store metadata
                self.message_metadata[offline_message['message_id']] = {
                    'username': username,
                    'stored_at': time.time(),
                    'size_bytes': len(str(message)),
                    'priority': message.get('priority', 3),
                    'message_type': 'offline_storage'
                }
                
                # Update statistics
                self.stats['total_messages_stored'] += 1
                self._update_user_stats()
                
                print(f"💾 Stored offline message for '{username}': '{message.get('text', '')[:30]}...' (Queue: {len(self.user_queues[username])})")
                
                return True
                
        except Exception as e:
            print(f"❌ Error storing offline message: {e}")
            return False
    
    def store_message_for_multiple_users(self, usernames: List[str], message: Dict) -> Dict:
        """
        Store a message for multiple offline users (like group messages)
        
        Args:
            usernames: List of target usernames
            message: Message dictionary
            
        Returns:
            Dict: Results for each user
        """
        results = {}
        
        for username in usernames:
            success = self.store_message_for_user(username, message)
            results[username] = {
                'stored': success,
                'queue_size': len(self.user_queues[username]) if success else 0
            }
        
        print(f"📨 Stored group message for {len(usernames)} users: {sum(1 for r in results.values() if r['stored'])} successful")
        
        return results
    
    def deliver_offline_messages(self, username: str, user_manager) -> List[Dict]:
        """
        Deliver all offline messages for a user when they come online
        
        Args:
            username: Username that came online
            user_manager: UserManager instance to check online status
            
        Returns:
            List[Dict]: Messages that were delivered
        """
        try:
            with self.lock:
                if username not in self.user_queues or not self.user_queues[username]:
                    print(f"📭 No offline messages for '{username}'")
                    return []
                
                # Check if user is actually online
                if not user_manager.users.get(username, {}).get('is_online', False):
                    print(f"⚠️ User '{username}' not marked as online, skipping delivery")
                    return []
                
                # Get all messages for user
                user_queue = self.user_queues[username]
                delivered_messages = []
                failed_deliveries = []
                
                # Process all messages in user's queue
                while user_queue:
                    offline_message = user_queue.popleft()
                    
                    # Check if message has expired
                    if time.time() > offline_message['expiry_timestamp']:
                        self.stats['total_messages_expired'] += 1
                        print(f"⏰ Expired message for '{username}': '{offline_message['original_message'].get('text', '')[:30]}...'")
                        
                        # Clean up metadata
                        if offline_message['message_id'] in self.message_metadata:
                            del self.message_metadata[offline_message['message_id']]
                        
                        continue
                    
                    # Attempt delivery
                    offline_message['delivery_attempts'] += 1
                    delivery_time = time.time()
                    
                    try:
                        # Mark as delivered
                        offline_message['is_delivered'] = True
                        offline_message['delivered_timestamp'] = delivery_time
                        offline_message['delivery_latency'] = delivery_time - offline_message['stored_timestamp']
                        
                        # Add delivery metadata to original message
                        message_for_delivery = offline_message['original_message'].copy()
                        message_for_delivery.update({
                            'offline_delivery': True,
                            'stored_duration': delivery_time - offline_message['stored_timestamp'],
                            'offline_message_id': offline_message['message_id'],
                            'was_offline_message': True
                        })
                        
                        delivered_messages.append(message_for_delivery)
                        self.stats['total_messages_delivered'] += 1
                        
                        print(f"📬 Delivered offline message to '{username}': '{message_for_delivery.get('text', '')[:30]}...'")
                        
                    except Exception as delivery_error:
                        print(f"❌ Failed to deliver message to '{username}': {delivery_error}")
                        failed_deliveries.append(offline_message)
                
                # Re-queue failed deliveries (if any)
                for failed_msg in failed_deliveries:
                    if failed_msg['delivery_attempts'] < 3:  # Max 3 delivery attempts
                        user_queue.append(failed_msg)
                    else:
                        print(f"💔 Permanently failed delivery for '{username}': {failed_msg['message_id']}")
                
                # Update statistics
                self._update_user_stats()
                
                # Call delivery callback if provided
                if self.delivery_callback and delivered_messages:
                    try:
                        self.delivery_callback(username, delivered_messages)
                    except Exception as e:
                        print(f"❌ Offline delivery callback error: {e}")
                
                print(f"📦 OFFLINE DELIVERY COMPLETE for '{username}': {len(delivered_messages)} messages delivered")
                
                return delivered_messages
                
        except Exception as e:
            print(f"❌ Error delivering offline messages: {e}")
            return []
    
    def get_offline_message_count(self, username: str) -> int:
        """
        Get number of offline messages waiting for a user
        
        Args:
            username: Username to check
            
        Returns:
            int: Number of pending messages
        """
        with self.lock:
            if username not in self.user_queues:
                return 0
            
            # Count non-expired messages
            current_time = time.time()
            valid_messages = 0
            
            for msg in self.user_queues[username]:
                if current_time <= msg['expiry_timestamp']:
                    valid_messages += 1
            
            return valid_messages
    
    def get_all_offline_users(self) -> List[Dict]:
        """
        Get all users who have offline messages waiting
        
        Returns:
            List[Dict]: User information with message counts
        """
        offline_users = []
        current_time = time.time()
        
        with self.lock:
            for username, queue in self.user_queues.items():
                if queue:  # Has messages
                    valid_messages = sum(1 for msg in queue if current_time <= msg['expiry_timestamp'])
                    
                    if valid_messages > 0:
                        oldest_message_time = min(msg['stored_timestamp'] for msg in queue 
                                                if current_time <= msg['expiry_timestamp'])
                        
                        offline_users.append({
                            'username': username,
                            'message_count': valid_messages,
                            'oldest_message_age': current_time - oldest_message_time,
                            'queue_size': len(queue)
                        })
        
        return sorted(offline_users, key=lambda x: x['oldest_message_age'], reverse=True)
    
    def peek_user_messages(self, username: str, limit: int = 5) -> List[Dict]:
        """
        Preview offline messages for a user without delivering them
        
        Args:
            username: Username to preview messages for
            limit: Maximum number of messages to preview
            
        Returns:
            List[Dict]: Preview of pending messages
        """
        with self.lock:
            if username not in self.user_queues:
                return []
            
            preview_messages = []
            current_time = time.time()
            
            for msg in list(self.user_queues[username])[:limit]:
                if current_time <= msg['expiry_timestamp']:
                    preview_messages.append({
                        'message_id': msg['message_id'],
                        'text': msg['original_message'].get('text', '')[:100],
                        'from_user': msg['original_message'].get('user', 'Unknown'),
                        'priority': msg['original_message'].get('priority', 3),
                        'stored_age': current_time - msg['stored_timestamp'],
                        'expires_in': msg['expiry_timestamp'] - current_time
                    })
            
            return preview_messages
    
    def clear_user_messages(self, username: str) -> int:
        """
        Clear all offline messages for a specific user
        
        Args:
            username: Username to clear messages for
            
        Returns:
            int: Number of messages cleared
        """
        with self.lock:
            if username not in self.user_queues:
                return 0
            
            cleared_count = len(self.user_queues[username])
            
            # Clean up metadata
            for msg in self.user_queues[username]:
                if msg['message_id'] in self.message_metadata:
                    del self.message_metadata[msg['message_id']]
            
            # Clear the queue
            self.user_queues[username].clear()
            
            # Remove empty queue
            if not self.user_queues[username]:
                del self.user_queues[username]
            
            self._update_user_stats()
            
            print(f"🗑️ Cleared {cleared_count} offline messages for '{username}'")
            return cleared_count
    
    def _cleanup_processor(self):
        """
        Background thread that cleans up expired messages
        """
        print("🧹 Offline queue cleanup thread started")
        
        while self.running:
            try:
                time.sleep(self.auto_cleanup_interval)
                
                if not self.running:
                    break
                
                self._cleanup_expired_messages()
                
            except Exception as e:
                print(f"❌ Cleanup processor error: {e}")
                time.sleep(10)  # Wait longer on error
    
    def _cleanup_expired_messages(self):
        """
        Remove expired messages from all user queues
        """
        try:
            with self.lock:
                current_time = time.time()
                total_expired = 0
                empty_users = []
                
                for username, queue in self.user_queues.items():
                    expired_messages = []
                    remaining_messages = deque(maxlen=self.max_messages_per_user)
                    
                    # Check each message in user's queue
                    while queue:
                        msg = queue.popleft()
                        
                        if current_time <= msg['expiry_timestamp']:
                            # Message still valid
                            remaining_messages.append(msg)
                        else:
                            # Message expired
                            expired_messages.append(msg)
                            total_expired += 1
                            
                            # Clean up metadata
                            if msg['message_id'] in self.message_metadata:
                                del self.message_metadata[msg['message_id']]
                    
                    # Update user's queue with remaining messages
                    self.user_queues[username] = remaining_messages
                    
                    # Mark users with no messages for removal
                    if not remaining_messages:
                        empty_users.append(username)
                    
                    if expired_messages:
                        print(f"⏰ Cleaned {len(expired_messages)} expired messages for '{username}'")
                
                # Remove empty user queues
                for username in empty_users:
                    del self.user_queues[username]
                
                # Update statistics
                if total_expired > 0:
                    self.stats['total_messages_expired'] += total_expired
                    self.stats['total_cleanup_runs'] += 1
                    self._update_user_stats()
                    
                    print(f"🧹 CLEANUP COMPLETE: {total_expired} expired messages removed, {len(empty_users)} empty queues cleared")
                
        except Exception as e:
            print(f"❌ Cleanup error: {e}")
    
    def _update_user_stats(self):
        """
        Update user-related statistics
        """
        with self.lock:
            self.stats['users_with_offline_messages'] = len(self.user_queues)
            
            if self.user_queues:
                total_messages = sum(len(queue) for queue in self.user_queues.values())
                self.stats['average_messages_per_user'] = total_messages / len(self.user_queues)
                
                # Find oldest undelivered message
                current_time = time.time()
                oldest_age = 0
                
                for queue in self.user_queues.values():
                    for msg in queue:
                        age = current_time - msg['stored_timestamp']
                        oldest_age = max(oldest_age, age)
                
                self.stats['oldest_undelivered_age'] = oldest_age
            else:
                self.stats['average_messages_per_user'] = 0.0
                self.stats['oldest_undelivered_age'] = 0.0
            
            # Calculate delivery success rate
            total_attempted = self.stats['total_messages_delivered'] + self.stats['total_messages_expired']
            if total_attempted > 0:
                self.stats['delivery_success_rate'] = (self.stats['total_messages_delivered'] / total_attempted) * 100
            else:
                self.stats['delivery_success_rate'] = 0.0
    
    def get_queue_status(self) -> Dict:
        """
        Get current offline queue status and statistics
        
        Returns:
            Dict: Current status information
        """
        with self.lock:
            # Calculate current totals
            total_pending_messages = sum(len(queue) for queue in self.user_queues.values())
            
            user_breakdown = {}
            for username, queue in self.user_queues.items():
                if queue:
                    user_breakdown[username] = {
                        'message_count': len(queue),
                        'oldest_message_age': time.time() - queue[0]['stored_timestamp'] if queue else 0
                    }
            
            return {
                'total_pending_messages': total_pending_messages,
                'users_with_messages': len(self.user_queues),
                'user_breakdown': user_breakdown,
                'is_running': self.running,
                'configuration': {
                    'max_messages_per_user': self.max_messages_per_user,
                    'message_expiry_hours': self.message_expiry_seconds / 3600,
                    'cleanup_interval_minutes': self.auto_cleanup_interval / 60
                },
                'statistics': self.stats.copy()
            }
    
    def force_cleanup(self) -> Dict:
        """
        Force immediate cleanup of expired messages
        
        Returns:
            Dict: Cleanup results
        """
        print("🧹 Forcing immediate cleanup...")
        
        before_stats = self.get_queue_status()
        self._cleanup_expired_messages()
        after_stats = self.get_queue_status()
        
        cleanup_results = {
            'messages_before': before_stats['total_pending_messages'],
            'messages_after': after_stats['total_pending_messages'],
            'messages_expired': before_stats['total_pending_messages'] - after_stats['total_pending_messages'],
            'users_before': before_stats['users_with_messages'],
            'users_after': after_stats['users_with_messages'],
            'cleanup_timestamp': time.time()
        }
        
        print(f"🧹 FORCED CLEANUP RESULTS: {cleanup_results['messages_expired']} messages expired")
        
        return cleanup_results
    
    def handle_user_online(self, username: str, user_manager) -> Optional[Dict]:
        """
        Handle when a user comes online - deliver their offline messages
        
        Args:
            username: Username that came online
            user_manager: UserManager instance
            
        Returns:
            Optional[Dict]: Delivery summary or None if no messages
        """
        message_count = self.get_offline_message_count(username)
        
        if message_count == 0:
            return None
        
        print(f"👋 User '{username}' came online with {message_count} offline messages waiting")
        
        # Deliver messages
        delivered_messages = self.deliver_offline_messages(username, user_manager)
        
        delivery_summary = {
            'username': username,
            'messages_delivered': len(delivered_messages),
            'delivery_timestamp': time.time(),
            'messages': delivered_messages
        }
        
        return delivery_summary
    
    def handle_user_offline(self, username: str) -> Dict:
        """
        Handle when a user goes offline - prepare for message storage
        
        Args:
            username: Username that went offline
            
        Returns:
            Dict: Offline preparation summary
        """
        current_count = self.get_offline_message_count(username)
        
        offline_summary = {
            'username': username,
            'went_offline_at': time.time(),
            'existing_offline_messages': current_count,
            'queue_ready': True
        }
        
        print(f"👤 User '{username}' went offline (had {current_count} pending messages)")
        
        return offline_summary
    
    def get_offline_message_count(self, username: str) -> int:
        """
        Get number of valid (non-expired) offline messages for a user
        
        Args:
            username: Username to check
            
        Returns:
            int: Number of pending messages
        """
        if username not in self.user_queues:
            return 0
        
        current_time = time.time()
        valid_count = 0
        
        for msg in self.user_queues[username]:
            if current_time <= msg['expiry_timestamp']:
                valid_count += 1
        
        return valid_count
    
    def clear_all_messages(self) -> Dict:
        """
        Clear all offline messages for all users
        
        Returns:
            Dict: Clear operation results
        """
        with self.lock:
            total_messages = sum(len(queue) for queue in self.user_queues.values())
            total_users = len(self.user_queues)
            
            # Clear all queues
            self.user_queues.clear()
            self.message_metadata.clear()
            
            # Reset relevant stats
            self.stats['users_with_offline_messages'] = 0
            self.stats['average_messages_per_user'] = 0.0
            self.stats['oldest_undelivered_age'] = 0.0
        
        clear_results = {
            'messages_cleared': total_messages,
            'users_affected': total_users,
            'clear_timestamp': time.time()
        }
        
        print(f"🧹 CLEARED ALL: {total_messages} messages for {total_users} users")
        
        return clear_results
    
    def update_config(self,
                     max_messages_per_user: Optional[int] = None,
                     message_expiry_hours: Optional[int] = None,
                     auto_cleanup_interval: Optional[int] = None):
        """
        Update offline queue configuration
        
        Args:
            max_messages_per_user: New max messages per user
            message_expiry_hours: New expiry time in hours
            auto_cleanup_interval: New cleanup interval in seconds
        """
        with self.lock:
            if max_messages_per_user is not None:
                self.max_messages_per_user = max(10, max_messages_per_user)
                
                # Update existing queues with new limit
                for username in self.user_queues:
                    new_queue = deque(self.user_queues[username], maxlen=self.max_messages_per_user)
                    self.user_queues[username] = new_queue
            
            if message_expiry_hours is not None:
                self.message_expiry_seconds = max(1, message_expiry_hours) * 3600
            
            if auto_cleanup_interval is not None:
                self.auto_cleanup_interval = max(60, auto_cleanup_interval)
        
        print(f"⚙️ Offline queue config updated: max_per_user={self.max_messages_per_user}, expiry={self.message_expiry_seconds/3600}h")
    
    def stop(self):
        """
        Stop the offline queue and cleanup thread
        """
        self.running = False
        
        # Wait for cleanup thread to finish
        if self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=2.0)
        
        print(f"⏹️ Offline queue stopped. Final stats: {self.stats}")
    
    def export_offline_log(self) -> Dict:
        """
        Export detailed offline queue log for analysis
        
        Returns:
            Dict: Comprehensive offline queue data
        """
        with self.lock:
            user_details = {}
            
            for username, queue in self.user_queues.items():
                messages_info = []
                for msg in queue:
                    messages_info.append({
                        'message_id': msg['message_id'],
                        'stored_timestamp': msg['stored_timestamp'],
                        'expiry_timestamp': msg['expiry_timestamp'],
                        'delivery_attempts': msg['delivery_attempts'],
                        'is_delivered': msg['is_delivered'],
                        'text_preview': msg['original_message'].get('text', '')[:50],
                        'priority': msg['original_message'].get('priority', 3)
                    })
                
                user_details[username] = {
                    'message_count': len(queue),
                    'messages': messages_info
                }
            
            return {
                'timestamp': datetime.now().isoformat(),
                'configuration': {
                    'max_messages_per_user': self.max_messages_per_user,
                    'message_expiry_hours': self.message_expiry_seconds / 3600,
                    'auto_cleanup_interval': self.auto_cleanup_interval
                },
                'current_state': {
                    'total_users_with_messages': len(self.user_queues),
                    'total_pending_messages': sum(len(queue) for queue in self.user_queues.values()),
                    'is_running': self.running
                },
                'statistics': self.stats.copy(),
                'user_details': user_details
            }


# Example callback function for offline message delivery
def offline_delivery_callback(username: str, delivered_messages: List[Dict]):
    """
    Example callback function for handling offline message delivery
    
    Args:
        username: Username that received messages
        delivered_messages: List of messages that were delivered
    """
    print(f"📨 OFFLINE DELIVERY CALLBACK: {username} received {len(delivered_messages)} messages")
    
    for msg in delivered_messages:
        print(f"   📬 {msg.get('user', 'Unknown')}: {msg.get('text', '')[:40]}...")


# Example usage and testing
if __name__ == "__main__":
    # Create offline queue with callback
    offline_queue = OfflineQueue(
        max_messages_per_user=50,
        message_expiry_hours=24,
        auto_cleanup_interval=60,  # 1 minute for testing
        delivery_callback=offline_delivery_callback
    )
    
    # Mock user manager for testing
    class MockUserManager:
        def __init__(self):
            self.users = {
                'alice': {'is_online': False},
                'bob': {'is_online': True},
                'charlie': {'is_online': False}
            }
        
        def set_user_online(self, username):
            if username in self.users:
                self.users[username]['is_online'] = True
        
        def set_user_offline(self, username):
            if username in self.users:
                self.users[username]['is_online'] = False
    
    user_manager = MockUserManager()
    
    # Test with sample messages
    sample_messages = [
        {"user": "Bob", "text": "Hey Alice, are you there?", "priority": 3, "timestamp": time.time()},
        {"user": "Charlie", "text": "Alice, urgent meeting tomorrow!", "priority": 1, "timestamp": time.time()},
        {"user": "Bob", "text": "Alice, please check the document", "priority": 2, "timestamp": time.time()},
        {"user": "System", "text": "Alice, you have new notifications", "priority": 3, "timestamp": time.time()},
    ]
    
    print("\n🧪 Testing offline queue...")
    
    # Store messages for offline user 'alice'
    for msg in sample_messages:
        offline_queue.store_message_for_user('alice', msg)
        time.sleep(0.1)
    
    # Check alice's offline messages
    alice_count = offline_queue.get_offline_message_count('alice')
    print(f"\n📊 Alice has {alice_count} offline messages")
    
    # Preview alice's messages
    preview = offline_queue.peek_user_messages('alice', 3)
    print(f"👀 Preview of Alice's messages: {len(preview)} shown")
    for p in preview:
        print(f"   📨 From {p['from_user']}: {p['text']}")
    
    # Simulate alice coming online
    print(f"\n👋 Alice comes online...")
    user_manager.set_user_online('alice')
    
    delivery_summary = offline_queue.handle_user_online('alice', user_manager)
    if delivery_summary:
        print(f"📦 Delivery summary: {delivery_summary['messages_delivered']} messages delivered")
    
    # Check status
    status = offline_queue.get_queue_status()
    print(f"\n📊 Final Status:")
    print(f"   📨 Total pending: {status['total_pending_messages']}")
    print(f"   👥 Users with messages: {status['users_with_messages']}")
    print(f"   📈 Success rate: {status['statistics']['delivery_success_rate']:.1f}%")
    
    # Test group message storage
    print(f"\n📢 Testing group message storage...")
    group_message = {"user": "Admin", "text": "Server maintenance tonight", "priority": 2}
    group_results = offline_queue.store_message_for_multiple_users(['bob', 'charlie'], group_message)
    
    for username, result in group_results.items():
        print(f"   {'✅' if result['stored'] else '❌'} {username}: {result['queue_size']} messages")
    
    # Wait a bit for background processing
    time.sleep(2)
    
    # Export log
    log_data = offline_queue.export_offline_log()
    print(f"\n📋 Export log contains {len(log_data['user_details'])} users")
    
    # Clean shutdown
    offline_queue.stop()