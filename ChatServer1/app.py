"""
Priority Chat System - Flask Backend with Batch, Retry, and Offline Queues
Real-time chat with priority queues, batch processing, retry mechanisms, and offline message storage
UPDATED: Added OfflineQueue integration for disconnected users
"""

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import logging
from dotenv import load_dotenv
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Callable
from collections import defaultdict
from collections import deque
from models.batch_queue import BatchQueue
from models.retry_queue import RetryQueue
from models.circular_queue import CircularQueue
from models.offline_queue import OfflineQueue  # NEW IMPORT
from models.user_manager import UserManager  # ADDED IMPORT
from utils.message_utils import validate_message, validate_username, format_message_for_client, create_system_message

# Load environment variables
load_dotenv()

class SpamDetector:
    """Spam detection for chat messages"""
    
    def __init__(self):
        self.spam_keywords = [
            "buy now", "free money", "visit this site",
            "click here", "subscribe", "lottery",
            "win cash", "make money fast", "100% free",
            "limited offer", "act now", "double your",
            "work from home", "earn $", "make $$$",
            "guaranteed", "no risk", "urgent", "congratulations",
            "winner", "claim now", "exclusive deal"
        ]
        
        self.repeated_char_pattern = re.compile(r"(.)\1{4,}")
        self.url_pattern = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)
        self.repeated_word_pattern = re.compile(r"\b(\w+)\s+\1\s+\1", re.IGNORECASE)
        self.caps_pattern = re.compile(r"[A-Z]{10,}")
    
    def normalize(self, text: str) -> str:
        """Remove punctuation/symbols and lowercase the text."""
        return re.sub(r"[^a-z0-9\s]", "", text.lower())
    
    def is_spam(self, text: str) -> bool:
        """Check if message is spam"""
        if not text:
            return False
        
        text_lower = text.lower()
        text_clean = self.normalize(text)
        
        # Keyword-based detection
        for keyword in self.spam_keywords:
            if keyword in text_clean:
                print(f"SPAM DETECTED: Keyword '{keyword}' in message: '{text[:50]}...'")
                return True
        
        # Repeated character spam
        if self.repeated_char_pattern.search(text_lower):
            print(f"SPAM DETECTED: Repeated characters in: '{text[:50]}...'")
            return True
        
        # Repeated word spam
        if self.repeated_word_pattern.search(text_lower):
            print(f"SPAM DETECTED: Repeated words in: '{text[:50]}...'")
            return True
        
        # URL detection
        if self.url_pattern.search(text):
            print(f"SPAM DETECTED: URL found in: '{text[:50]}...'")
            return True
        
        # Message length check
        if len(text) > 300:
            print(f"SPAM DETECTED: Message too long ({len(text)} chars): '{text[:50]}...'")
            return True
        
        # Too many capitals
        if self.caps_pattern.search(text):
            print(f"SPAM DETECTED: Too many capitals in: '{text[:50]}...'")
            return True
        
        return False


class DisplayMessageQueue:
    """
    Message queue that only displays messages after batch processing
    """
    
    def __init__(self, max_messages_per_category=50):
        self.max_messages_per_category = max_messages_per_category
        
        # Separate queues for each priority level (what users see)
        self.display_queues = {
            1: deque(maxlen=max_messages_per_category),  # URGENT
            2: deque(maxlen=max_messages_per_category),  # HIGH  
            3: deque(maxlen=max_messages_per_category),  # NORMAL
            4: deque(maxlen=max_messages_per_category)   # LOW/SPAM
        }
        
        # Priority names mapping
        self.priority_names = {
            1: "URGENT",
            2: "HIGH", 
            3: "NORMAL",
            4: "LOW"
        }
        
        # Message counter for unique IDs
        self.message_counter = 0
    
    def add_batch_messages(self, messages: List[Dict]):
        """Add messages from completed batch to display queues"""
        added_count = 0
        
        for message_data in messages:
            priority = message_data.get("priority", 3)
            
            # Add unique message ID if not present
            if "id" not in message_data:
                self.message_counter += 1
                message_data["id"] = self.message_counter
            
            # Add to appropriate display queue
            self.display_queues[priority].append(message_data)
            added_count += 1
            
            print(f"Added to DISPLAY: {self.priority_names[priority]} - '{message_data['text'][:30]}...'")
        
        print(f"BATCH DISPLAYED: {added_count} messages now visible to users")
        return added_count
    
    def get_all_messages_organized(self):
        """Get all messages organized by priority (what users see)"""
        organized_messages = {
            "urgent": list(self.display_queues[1]),
            "high": list(self.display_queues[2]), 
            "normal": list(self.display_queues[3]),
            "low": list(self.display_queues[4])
        }
        
        return organized_messages
    
    def get_queue_stats(self):
        """Get statistics about display queues"""
        return {
            "urgent_count": len(self.display_queues[1]),
            "high_count": len(self.display_queues[2]),
            "normal_count": len(self.display_queues[3]),
            "low_count": len(self.display_queues[4]),
            "total_messages": sum(len(queue) for queue in self.display_queues.values())
        }
    
    def clear_all(self):
        """Clear all display queues"""
        for queue in self.display_queues.values():
            queue.clear()
        self.message_counter = 0


def detect_message_priority(message: str, manual_priority: int = None) -> int:
    """Auto-detect message priority based on content analysis"""
    # If manual priority is set and valid, use it
    if manual_priority is not None and manual_priority in [1, 2, 3, 4]:
        return manual_priority
    
    if not message:
        return 3  # NORMAL
    
    message_lower = message.lower()
    
    # URGENT priority keywords
    urgent_keywords = [
        'urgent', 'emergency', 'asap', 'immediately', 'critical',
        'help', 'issue', 'problem', 'error', 'bug', 'down',
        'broken', 'failed', 'crash', 'alert'
    ]
    
    # HIGH priority keywords  
    high_keywords = [
        'important', 'priority', 'deadline', 'meeting', 'review',
        'approval', 'decision', 'update', 'status', 'progress'
    ]
    
    # Check for ALL CAPS (indicates urgency/emphasis)
    caps_ratio = sum(1 for c in message if c.isupper()) / len(message) if message else 0
    if caps_ratio > 0.7 and len(message) > 10:
        return 1  # URGENT
    
    # Check for @mentions (indicates direct communication)
    if '@' in message:
        return 2  # HIGH
    
    # Check for urgent keywords
    for keyword in urgent_keywords:
        if keyword in message_lower:
            return 1  # URGENT
    
    # Check for high priority keywords
    for keyword in high_keywords:
        if keyword in message_lower:
            return 2  # HIGH
    
    # Default priority
    return 3  # NORMAL


def create_app():
    """Application factory pattern"""
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
    
    if app.debug:
        logging.basicConfig(level=logging.DEBUG)
    
    return app


def create_socketio(app):
    """Create SocketIO instance"""
    return SocketIO(
        app,
        cors_allowed_origins="*",
        logger=False,
        engineio_logger=False,
        async_mode='threading'
    )


# Create Flask app and SocketIO
app = create_app()
socketio = create_socketio(app)

# Initialize components
spam_detector = SpamDetector()
display_queue = DisplayMessageQueue()
user_manager = UserManager()  # NEW: Initialize user manager


# NEW: Offline message delivery callback
def offline_delivery_callback(username: str, delivered_messages: List[Dict]):
    """
    Handle offline message delivery via SocketIO
    
    Args:
        username: Username that received messages
        delivered_messages: List of messages that were delivered
    """
    try:
        if username in user_manager.users and user_manager.users[username].socket_id:
            # Send offline messages to the specific user
            socketio.emit('offline_messages_delivered', {
                'message_count': len(delivered_messages),
                'messages': delivered_messages,
                'delivery_timestamp': time.time()
            }, room=user_manager.users[username].socket_id)
            
            print(f"Sent {len(delivered_messages)} offline messages to {username}")
        
    except Exception as e:
        print(f"Offline delivery callback error: {e}")


# Batch transmission callback
def batch_transmission_callback(batch_data: Dict):
    """Handle batch transmission via SocketIO"""
    try:
        print(f"TRANSMITTING BATCH: {batch_data['batch_id']} ({batch_data['batch_size']} messages)")
        
        # Add batch messages to display queue (users can now see them)
        display_queue.add_batch_messages(batch_data['messages'])
        
        # Emit the updated organization (this is when users see messages)
        organized_messages = display_queue.get_all_messages_organized()
        socketio.emit("message_organization", organized_messages)
        
        # Emit batch update notification
        socketio.emit('batch_update', {
            'batch_id': batch_data['batch_id'],
            'batch_size': batch_data['batch_size'],
            'send_reason': batch_data['send_reason'],
            'efficiency_stats': {
                'wait_time': batch_data['wait_time'],
                'efficiency_score': min(100, (batch_data['batch_size'] / batch_queue.max_batch_size) * 100)
            }
        })
        
        print(f"Batch transmitted and displayed to users")
        
    except Exception as e:
        print(f"Batch transmission error: {e}")


# Retry success callback
def retry_success_callback(retry_entry: Dict, total_retry_time: float):
    """Handle successful message retry"""
    try:
        print(f"MESSAGE RETRY SUCCESS: {retry_entry['retry_id']}")
        
        # Add the successfully retried message back to batch queue
        message = retry_entry['message']
        message['retry_success'] = True
        message['retry_attempts'] = retry_entry['retry_count']
        message['retry_time'] = total_retry_time
        
        # Add to batch queue (will be displayed when batch is sent)
        batch_queue.add_message(message)
        
        socketio.emit('retry_success', {
            'message_id': message.get('id'),
            'retry_attempts': retry_entry['retry_count'],
            'total_time': total_retry_time
        })
        
    except Exception as e:
        print(f"Retry success callback error: {e}")


# Retry failure callback  
def retry_failure_callback(retry_entry: Dict, failure_reason: str):
    """Handle permanent message retry failure"""
    try:
        print(f"MESSAGE RETRY FAILED PERMANENTLY: {retry_entry['retry_id']}")
        
        socketio.emit('retry_failure', {
            'message_id': retry_entry['message'].get('id'),
            'retry_attempts': retry_entry['retry_count'],
            'failure_reason': failure_reason,
            'original_text': retry_entry['message'].get('text', '')[:50]
        })
        
        print(f"Message moved to dead letter queue: '{retry_entry['message'].get('text', '')[:50]}...'")
        
    except Exception as e:
        print(f"Retry failure callback error: {e}")


# Initialize all queues
batch_queue = BatchQueue(
    min_batch_size=5,
    max_batch_size=25,
    max_wait_time=2.0,
    callback=batch_transmission_callback
)

retry_queue = RetryQueue(
    max_retries=3,
    initial_delay=1.0,
    max_delay=30.0,
    backoff_multiplier=2.0,
    success_callback=retry_success_callback,
    failure_callback=retry_failure_callback
)

circular_queue = CircularQueue(max_size=1000)

# NEW: Initialize offline queue
offline_queue = OfflineQueue(
    max_messages_per_user=100,
    message_expiry_hours=24,
    auto_cleanup_interval=300,
    delivery_callback=offline_delivery_callback
)

# Link offline queue with user manager
user_manager.set_offline_queue(offline_queue)

# Get access code from environment
ACCESS_CODE = os.getenv("CHAT_ACCESS_CODE", "supersecret123")


def simulate_message_delivery(message: Dict) -> bool:
    """Simulate message delivery for testing retry queue"""
    import random
    
    # Simulate different failure rates based on priority
    if message.get('priority') == 1:  # URGENT
        success_rate = 0.9
    elif message.get('priority') == 2:  # HIGH
        success_rate = 0.85
    elif message.get('priority') == 3:  # NORMAL
        success_rate = 0.8
    else:  # LOW/SPAM
        success_rate = 0.7
    
    return random.random() < success_rate


@socketio.on("connect")
def handle_connect():
    print(f"Client connected: {request.sid}")
    
    # Send current message organization to new client
    organized_messages = display_queue.get_all_messages_organized()
    emit("message_organization", organized_messages)
    
    # Send current queue stats
    emit("batch_stats", batch_queue.get_queue_status())
    emit("retry_stats", retry_queue.get_queue_status())
    emit("offline_stats", offline_queue.get_queue_status())  # NEW


@socketio.on("disconnect")
def handle_disconnect():
    print(f"Client disconnected: {request.sid}")
    
    # Handle user going offline through user manager
    username = user_manager.set_user_offline(request.sid)
    # Note: offline_queue.handle_user_offline() is now called automatically 
    # by user_manager.set_user_offline() since they're linked


@socketio.on("login")
def handle_login(data):
    """Handle user login with secret key and user registration"""
    try:
        username = data.get("username", "").strip()
        secret = data.get("secretKey", "").strip()
        
        if not username or not secret:
            emit("login_error", "Username and secret key required")
            return
        
        if secret != ACCESS_CODE:
            emit("login_error", "Invalid secret key")
            print(f"Failed login attempt - User: {username}, Key: {secret}")
            return
        
        # NEW: Register or login user through UserManager
        if user_manager.is_username_available(username):
            # Register new user
            result = user_manager.register_user(username, "default_password")
            if not result['success']:
                emit("login_error", result['error'])
                return
        else:
            # Login existing user
            result = user_manager.login_user(username, "default_password")
            if not result['success']:
                emit("login_error", result['error'])
                return
        
        # Set user online
        user_manager.set_user_online(username, request.sid)
        
        # NEW: Check for offline messages and deliver them
        delivery_summary = offline_queue.handle_user_online(username, user_manager)
        
        emit("login_success", {"username": username})
        print(f"User '{username}' logged in successfully")
        
        # NEW: Notify about offline messages if any were delivered
        if delivery_summary and delivery_summary['messages_delivered'] > 0:
            emit("offline_messages_notification", {
                'count': delivery_summary['messages_delivered'],
                'message': f"You received {delivery_summary['messages_delivered']} messages while offline"
            })
        
        # Send current message organization to logged in user
        organized_messages = display_queue.get_all_messages_organized()
        emit("message_organization", organized_messages)
        
        # Send queue statistics
        emit("batch_stats", batch_queue.get_queue_status())
        emit("retry_stats", retry_queue.get_queue_status())
        emit("offline_stats", offline_queue.get_queue_status())  # NEW
        
    except Exception as e:
        print(f"Login error: {e}")
        emit("login_error", "Login failed")


@socketio.on("send_message")
def handle_message(data):
    """Handle incoming messages with offline user support"""
    try:
        # Get username from UserManager
        username = user_manager.get_user_by_socket(request.sid)
        if not username:
            print(f"Message from unauthorized user: {request.sid}")
            return
        
        message = data.get("message", "").strip()
        manual_priority = data.get("priority")
        
        if not message:
            return
        
        print(f"NEW MESSAGE from '{username}': '{message}'")
        
        # Validate message
        validation_result = validate_message(message)
        if not validation_result['valid']:
            emit("message_error", validation_result['error'])
            return
        
        message = validation_result['sanitized_message']
        
        # Convert manual priority
        try:
            manual_priority = int(manual_priority) if manual_priority is not None else None
            if manual_priority is not None and manual_priority not in [1, 2, 3, 4]:
                manual_priority = None
        except (ValueError, TypeError):
            manual_priority = None
        
        # Auto-detect priority
        priority = detect_message_priority(message, manual_priority)
        
        # Check for spam
        is_spam = spam_detector.is_spam(message)
        if is_spam:
            priority = 4
            alert_message = "SPAM DETECTED"
            print(f"SPAM MESSAGE from '{username}': '{message}'")
        else:
            alert_message = None
        
        priority_names = {
            1: "URGENT",
            2: "HIGH", 
            3: "NORMAL",
            4: "LOW"
        }
        
        msg_data = {
            "user": username,
            "text": message,
            "priority": priority,
            "priority_name": priority_names.get(priority, "NORMAL"),
            "is_spam": is_spam,
            "alert_message": alert_message,
            "timestamp": datetime.now().isoformat(),
            "session_id": request.sid
        }
        
        # Update user activity
        user_manager.update_user_activity(username)
        
        # Add to circular history for record keeping
        circular_queue.enqueue(msg_data.copy())
        
        # Add to batch queue for online users
        batch_queue.add_message(msg_data.copy())
        
        # NEW: Store message for offline users (if this is a broadcast or group message)
        offline_users = []
        for user_obj in user_manager.users.values():
            if not user_obj.is_online:
                offline_users.append(user_obj.username)
        
        if offline_users:
            offline_queue.store_message_for_multiple_users(offline_users, msg_data.copy())
            print(f"Stored message for {len(offline_users)} offline users")
        
        # Simulate message delivery attempt (for retry queue testing)
        delivery_success = simulate_message_delivery(msg_data)
        
        if not delivery_success:
            # Add to retry queue if delivery failed
            retry_queue.add_failed_message(
                msg_data.copy(), 
                error_reason="simulated_network_failure",
                original_timestamp=time.time()
            )
        
        # Send confirmation to sender only
        emit("message_queued", {
            "status": "Message queued for batch processing",
            "priority": priority_names.get(priority),
            "batch_queue_size": len(batch_queue.get_pending_messages()),
            "estimated_wait": f"Waiting for {5 - len(batch_queue.current_batch)} more messages or {batch_queue.max_wait_time}s timeout",
            "offline_users_notified": len(offline_users)  # NEW
        })
        
        print(f"Message QUEUED: Priority={priority_names.get(priority)}, Queue={len(batch_queue.get_pending_messages())}")
        
    except Exception as e:
        print(f"Message handling error: {e}")


# NEW: Socket events for offline queue
@socketio.on("request_offline_stats")
def handle_request_offline_stats():
    """Send current offline queue statistics"""
    stats = offline_queue.get_queue_status()
    emit("offline_stats", stats)


@socketio.on("request_my_offline_count")
def handle_request_my_offline_count():
    """Get offline message count for current user"""
    username = user_manager.get_user_by_socket(request.sid)
    if username:
        count = offline_queue.get_offline_message_count(username)
        emit("my_offline_count", {"count": count, "username": username})


@socketio.on("preview_my_offline_messages")
def handle_preview_my_offline_messages():
    """Preview offline messages for current user"""
    username = user_manager.get_user_by_socket(request.sid)
    if username:
        preview = offline_queue.peek_user_messages(username, 5)
        emit("offline_messages_preview", {"messages": preview, "username": username})


@socketio.on("clear_my_offline_messages")
def handle_clear_my_offline_messages():
    """Clear offline messages for current user"""
    username = user_manager.get_user_by_socket(request.sid)
    if username:
        cleared_count = offline_queue.clear_user_messages(username)
        emit("offline_messages_cleared", {"cleared_count": cleared_count, "username": username})


# Existing socket events
@socketio.on("request_messages")
def handle_request_messages():
    """Send current message organization to requesting client"""
    organized_messages = display_queue.get_all_messages_organized()
    emit("message_organization", organized_messages)


@socketio.on("request_batch_stats")
def handle_request_batch_stats():
    """Send current batch queue statistics"""
    stats = batch_queue.get_queue_status()
    emit("batch_stats", stats)


@socketio.on("request_retry_stats")
def handle_request_retry_stats():
    """Send current retry queue statistics"""
    stats = retry_queue.get_queue_status()
    emit("retry_stats", stats)


@socketio.on("force_batch_send")
def handle_force_batch_send():
    """Force send current batch (admin function)"""
    username = user_manager.get_user_by_socket(request.sid)
    if username:
        batch_data = batch_queue.force_send_batch()
        if batch_data:
            emit("batch_forced", {
                'batch_id': batch_data['batch_id'],
                'batch_size': batch_data['batch_size']
            }, broadcast=True)
        else:
            emit("batch_force_result", {"success": False, "reason": "No messages in batch"})


@socketio.on("force_retry_all")
def handle_force_retry_all():
    """Force retry all waiting messages (admin function)"""
    username = user_manager.get_user_by_socket(request.sid)
    if username:
        moved_count = retry_queue.force_retry_all()
        emit("retry_forced", {"messages_moved": moved_count}, broadcast=True)


# Routes
@app.route('/')
def index():
    """Serve the chat interface"""
    return render_template('index.html')


@app.route('/health')
def health_check():
    """Health check endpoint"""
    queue_stats = display_queue.get_queue_stats()
    batch_stats = batch_queue.get_queue_status()
    retry_stats = retry_queue.get_queue_status()
    offline_stats = offline_queue.get_queue_status()  # NEW
    user_stats = user_manager.get_user_stats()  # NEW
    
    return {
        'status': 'healthy',
        'system': 'Priority Chat System with Batch, Retry & Offline Queues',
        'version': '3.0.0-WITH-OFFLINE',
        'connected_users': user_stats['currently_online'],
        'registered_users': user_stats['total_registered'],
        'offline_users': user_stats['currently_offline'],  # NEW
        'offline_queue_linked': user_stats['offline_queue_active'],  # NEW
        'display_queue_stats': queue_stats,
        'batch_stats': {
            'queue_size': batch_stats['queue_size'],
            'current_batch_size': batch_stats['current_batch_size'],
            'total_batches_sent': batch_stats['statistics']['total_batches_sent']
        },
        'retry_stats': {
            'retry_queue_size': retry_stats['retry_queue_size'],
            'waiting_queue_size': retry_stats['waiting_queue_size'],
            'success_rate': retry_stats['statistics']['total_messages_succeeded'] / max(1, retry_stats['statistics']['total_messages_added']) * 100
        },
        'offline_stats': {
            'users_with_offline_messages': offline_stats['users_with_messages'],
            'total_offline_messages': offline_stats['total_pending_messages'],
            'delivery_success_rate': offline_stats['statistics']['delivery_success_rate']
        },
        'user_management': {  # NEW
            'total_users': user_stats['total_registered'],
            'online_count': user_stats['currently_online'],
            'offline_count': user_stats['currently_offline'],
            'offline_messages_pending': user_stats['total_offline_messages_pending']
        }
    }

@app.route('/stats')
def get_stats():
    """Get comprehensive system statistics"""
    queue_stats = display_queue.get_queue_stats()
    batch_stats = batch_queue.get_queue_status()
    retry_stats = retry_queue.get_queue_status()
    offline_stats = offline_queue.get_queue_status()  # NEW
    user_stats = user_manager.get_user_stats()  # NEW
    
    return {
        'user_management': user_stats,  # NEW
        'connected_users': user_stats['currently_online'],
        'access_code_configured': bool(ACCESS_CODE),
        'spam_detection': 'active',
        'display_queues': queue_stats,
        'batch_processing': batch_stats,
        'retry_processing': retry_stats,
        'offline_processing': offline_stats,  # NEW
        'circular_queue_size': circular_queue.size(),
        'uptime': 'running'
    }


# NEW: Offline queue API endpoints
@app.route('/offline-stats')
def get_offline_stats():
    """API endpoint for detailed offline queue statistics"""
    return offline_queue.get_queue_status()


@app.route('/offline-users')
def get_offline_users():
    """API endpoint for users with offline messages"""
    return {"users": offline_queue.get_all_offline_users()}


@app.route('/offline-messages/<username>')
def get_user_offline_messages(username):
    """API endpoint to preview offline messages for a specific user"""
    preview = offline_queue.peek_user_messages(username, 10)
    count = offline_queue.get_offline_message_count(username)
    
    return {
        "username": username,
        "total_count": count,
        "preview": preview
    }


@app.route('/force-offline-cleanup')
def force_offline_cleanup():
    """Force cleanup of expired offline messages"""
    cleanup_results = offline_queue.force_cleanup()
    return cleanup_results


@app.route('/clear-offline-messages/<username>')
def clear_user_offline_messages(username):
    """Clear all offline messages for a specific user"""
    cleared_count = offline_queue.clear_user_messages(username)
    return {
        "username": username,
        "cleared_count": cleared_count,
        "timestamp": datetime.now().isoformat()
    }


# Existing API endpoints
@app.route('/batch-stats')
def get_batch_stats():
    """API endpoint for detailed batch statistics"""
    return batch_queue.get_queue_status()


@app.route('/retry-stats')
def get_retry_stats():
    """API endpoint for detailed retry statistics"""
    return retry_queue.get_queue_status()


@app.route('/retry-pending')
def get_retry_pending():
    """API endpoint for pending retry messages"""
    return {"pending_retries": retry_queue.get_pending_retries()}


@app.route('/messages')
def get_messages():
    """API endpoint to get organized messages (what users actually see)"""
    organized_messages = display_queue.get_all_messages_organized()
    return organized_messages


@app.route('/batch-queue')
def get_batch_queue():
    """API endpoint to see what's waiting in batch queue"""
    pending = batch_queue.get_pending_messages()
    return {
        "pending_messages": pending,
        "count": len(pending),
        "current_batch_size": len(batch_queue.current_batch),
        "queue_size": len(batch_queue.message_queue)
    }


@app.route('/export-logs')
def export_logs():
    """Export comprehensive system logs"""
    return {
        'export_timestamp': datetime.now().isoformat(),
        'batch_log': batch_queue.export_batch_log(),
        'retry_log': retry_queue.export_retry_log(),
        'offline_log': offline_queue.export_offline_log(),  # NEW
        'display_stats': display_queue.get_queue_stats(),
        'user_stats': user_manager.get_user_stats(),  # NEW
        'connected_users_count': len(user_manager.online_users)  # NEW
    }


@app.route('/clear-display')
def clear_display():
    """Clear all displayed messages (admin function)"""
    display_queue.clear_all()
    
    # Broadcast empty organization to all clients (FIXED: removed broadcast parameter)
    organized_messages = display_queue.get_all_messages_organized()
    socketio.emit("message_organization", organized_messages)
    
    return {"status": "Display queues cleared", "timestamp": datetime.now().isoformat()}

# NEW: Enhanced offline queue API endpoints
@app.route('/user-activity-report')
def get_user_activity_report():
    """API endpoint for comprehensive user activity report"""
    return user_manager.get_user_activity_report()


@app.route('/users-for-broadcast')
def get_users_for_broadcast():
    """API endpoint to see online vs offline users for broadcasting"""
    return user_manager.get_users_for_broadcast()


@app.route('/user-info/<username>')
def get_user_info_route(username):
    """API endpoint for detailed user information"""
    user_info = user_manager.get_user_info(username)
    if user_info:
        return user_info
    else:
        return {"error": "User not found"}, 404


@app.route('/offline-user-summary/<username>')
def get_offline_user_summary_route(username):
    """API endpoint for offline summary of specific user"""
    return user_manager.get_user_offline_summary(username)


@app.route('/cleanup-inactive-sessions')
def cleanup_inactive_sessions():
    """Force cleanup of inactive user sessions"""
    cleaned_count = user_manager.cleanup_inactive_sessions(max_inactive_hours=24)
    return {
        "sessions_cleaned": cleaned_count,
        "cleanup_timestamp": datetime.now().isoformat()
    }


@app.route('/force-user-offline/<username>')
def force_user_offline_route(username):
    """Force a user offline (admin function)"""
    success = user_manager.force_user_offline(username)
    return {
        "success": success,
        "username": username,
        "timestamp": datetime.now().isoformat()
    }


@app.route('/all-users-summary')
def get_all_users_summary():
    """API endpoint for summary of all users"""
    return {"users": user_manager.get_all_users_summary()}


def main():
    """Main application entry point"""
    print("=" * 60)
    print("🚀 Enhanced Priority Queue Chat System with OFFLINE QUEUE Starting...")
    print("=" * 60)
    print("📍 Main Interface: http://localhost:5000")
    print("📊 Health Check:  http://localhost:5000/health")
    print("📈 Statistics:    http://localhost:5000/stats")
    print("💬 Messages API:  http://localhost:5000/messages")
    print("📦 Batch Stats:   http://localhost:5000/batch-stats")
    print("📦 Batch Queue:   http://localhost:5000/batch-queue")
    print("💾 Offline Stats: http://localhost:5000/offline-stats")
    print("👥 Offline Users: http://localhost:5000/offline-users")
    print("=" * 60)
    print("🆕 NEW USER MANAGEMENT ENDPOINTS:")
    print("👤 User Activity: http://localhost:5000/user-activity-report")
    print("📡 Broadcast Info: http://localhost:5000/users-for-broadcast") 
    print("👥 All Users: http://localhost:5000/all-users-summary")
    print("=" * 60)
    print("🎯 Priority Queue Features:")
    print("  🔴 URGENT Messages   (Top Priority)")
    print("  🟡 HIGH Messages     (Second Priority)")
    print("  🟢 NORMAL Messages   (Third Priority)")
    print("  ⚫ LOW/SPAM Messages (Bottom Priority)")
    print("=" * 60)
    print("🆕 NEW OFFLINE QUEUE FEATURES:")
    print("  💾 Stores messages for disconnected users")
    print("  📬 Auto-delivers when users reconnect")
    print("  ⏰ Messages expire after 24 hours")
    print("  🧹 Automatic cleanup every 5 minutes")
    print("  👥 Supports up to 100 messages per user")
    print("  📊 Real-time offline message monitoring")
    print("=" * 60)
    print("🛠️ Queue System:")
    print("  📦 Messages ONLY appear after batch processing")
    print("  ⏳ Users wait for 5 messages OR 2 seconds")
    print("  🔄 Failed messages go to retry queue")
    print("  💾 Messages for offline users stored separately")
    print("  ✅ All queue operations are thread-safe")
    print("=" * 60)
    print("🆕 Offline Queue Testing URLs:")
    print("  💾 /offline-stats - View offline queue statistics")
    print("  👥 /offline-users - See users with pending messages")
    print("  📨 /offline-messages/<username> - Preview user's offline messages")
    print("  🧹 /clear-offline-messages/<username> - Clear user's messages")
    print("  🧹 /force-offline-cleanup - Force expire old messages")
    print("=" * 60)
    print("✨ Auto-Detection:")
    print("  ✅ Keywords: 'urgent', 'emergency', 'help', 'error', etc.")
    print("  ✅ ALL CAPS messages (>70% capitals)")
    print("  ✅ @mentions for HIGH priority")
    print("  ✅ Spam detection forces LOW priority")
    print("  ✅ Manual priority override available")
    print("=" * 60)
    print(f"🔑 Access Code: {ACCESS_CODE}")
    print("=" * 60)
    
    try:
        socketio.run(
            app,
            debug=True,
            port=5000,
            host='127.0.0.1',
            allow_unsafe_werkzeug=True
        )
    except KeyboardInterrupt:
        print("\n👋 Priority Chat system shutting down...")
        # Clean shutdown of all queues
        batch_queue.stop()
        retry_queue.stop()
        offline_queue.stop()  # NEW
    except Exception as e:
        print(f"🚨 Fatal error: {str(e)}")
        # Clean shutdown on error
        batch_queue.stop()
        retry_queue.stop()
        offline_queue.stop()  # NEW


if __name__ == '__main__':
    main()