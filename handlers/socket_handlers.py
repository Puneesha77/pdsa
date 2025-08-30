"""
Socket Event Handlers for Chat System
Now includes BatchQueue + RetryQueue + SessionQueue integration.
"""

from flask_socketio import emit
from utils.message_utils import validate_message, validate_username, format_message_for_client
from typing import Dict
from models.batch_queue import BatchQueue
from models.retry_queue import RetryQueue
from models.session_queue import SessionQueue


class ChatSocketHandlers:
    """
    Handles all WebSocket events for the chat system
    with BatchQueue + RetryQueue + SessionQueue integration
    """
    
    def __init__(self, message_system, socketio):
        self.message_system = message_system
        self.socketio = socketio
        self.connected_users = {}

        # Queues
        self.batch_queue = BatchQueue(min_batch_size=5, max_batch_size=50)
        self.retry_queue = RetryQueue(max_retries=5)
        self.session_queue = SessionQueue()

        # Register all socket event handlers
        self.register_handlers()
    
    def register_handlers(self):
        """Register all socket event handlers"""
        
        @self.socketio.on('connect')
        def handle_connect():
            return self.on_connect()
        
        @self.socketio.on('disconnect') 
        def handle_disconnect():
            return self.on_disconnect()
        
        @self.socketio.on('send_message')
        def handle_send_message(data):
            return self.on_send_message(data)
        
        @self.socketio.on('get_stats')
        def handle_get_stats():
            return self.on_get_stats()
        
        @self.socketio.on_error_default
        def handle_error(e):
            return self.on_error(e)
    
    def on_connect(self):
        """Handle new user connection"""
        print('✅ New user connected to Priority Chat!')

        # 🔑 Add login event to session queue
        self.session_queue.enqueue("Anonymous", "login")

        history = self.message_system.history.get_all()
        print(f'📊 Sending {len(history)} historical messages to new user')

        for msg in history:
            formatted_msg = format_message_for_client(msg)
            emit('new_message', formatted_msg)

        stats = self.message_system.get_queue_stats()
        emit('queue_stats', stats)
        
        return True

    def on_disconnect(self):
        """Handle user disconnection"""
        print('❌ User disconnected from Priority Chat')

        # 🔑 Add logout event to session queue
        self.session_queue.enqueue("Anonymous", "logout")

        return True
    
    def on_send_message(self, data: Dict):
        """Handle incoming message from user"""
        try:
            raw_message = data.get('message', '')
            raw_user = data.get('user', 'Anonymous')
            manual_priority = data.get('priority', None)

            print(f'📨 Processing message from {raw_user}: "{raw_message}"')

            # ✅ Validate message
            message_validation = validate_message(raw_message)
            if not message_validation['valid']:
                emit('error', {
                    'type': 'validation_error',
                    'message': message_validation['error']
                })
                return False

            # ✅ Validate username
            user_validation = validate_username(raw_user)
            clean_user = user_validation['sanitized_username']
            clean_message = message_validation['sanitized_message']

            # ✅ Priority override (if provided)
            if manual_priority is not None:
                try:
                    manual_priority = int(manual_priority)
                    if manual_priority not in [1, 2, 3, 4]:
                        manual_priority = None
                except (ValueError, TypeError):
                    manual_priority = None

            # ✅ Add message to priority system
            message_obj = self.message_system.add_message(
                clean_message, 
                clean_user, 
                manual_priority
            )

            # ✅ Get next message in priority order
            next_message = self.message_system.get_next_message()
            if not next_message:
                return False

            formatted_msg = format_message_for_client(next_message)

            # 🔹 Instead of sending directly → enqueue into BatchQueue
            self.batch_queue.enqueue(formatted_msg)

            # 🔹 Try flushing if enough messages in batch
            batch = self.batch_queue.flush()
            if batch:
                try:
                    emit('new_message_batch', batch, broadcast=True)
                    print(f"✅ Broadcasted batch of {len(batch)} messages")
                except Exception as e:
                    print(f"🚨 Batch send failed: {str(e)}")
                    for msg in batch:
                        self.retry_queue.enqueue(msg)

            # 🔹 Process RetryQueue (resend failed messages if ready)
            self.process_retry_queue()

            return True

        except Exception as e:
            print(f'🚨 Error processing message: {str(e)}')
            emit('error', {
                'type': 'processing_error', 
                'message': 'Failed to process message'
            })
            return False

    def process_retry_queue(self):
        """Check and resend failed messages when backoff is ready"""
        while True:
            retry_msg = self.retry_queue.process()
            if not retry_msg:
                break
            try:
                emit('new_message', retry_msg, broadcast=True)
                print("🔁 Retried and delivered a failed message")
            except Exception as e:
                print(f"🚨 Retry send failed again: {str(e)}")
                self.retry_queue.enqueue(retry_msg)

    def on_get_stats(self):
        """Handle request for queue statistics"""
        try:
            stats = self.message_system.get_queue_stats()
            stats.update({
                "batch_queue_size": self.batch_queue.size(),
                "retry_queue_size": self.retry_queue.size(),
                "session_queue_size": self.session_queue.size()
            })
            emit('queue_stats', stats)
            print(f'📊 Sent queue stats: {stats}')
        except Exception as e:
            print(f'🚨 Error getting stats: {str(e)}')
            emit('error', {'type': 'stats_error', 'message': 'Failed to get statistics'})
    
    def on_error(self, e):
        """Handle socket errors"""
        print(f'🚨 Socket Error: {str(e)}')
        return True

    def broadcast_system_message(self, message: str, priority: int = 3):
        """Broadcast a system message to all users"""
        try:
            system_msg = self.message_system.add_message(
                message, "SYSTEM", manual_priority=priority
            )
            next_message = self.message_system.get_next_message()
            if next_message:
                formatted_msg = format_message_for_client(next_message)
                self.batch_queue.enqueue(formatted_msg)
                # Force flush for system messages (immediate delivery)
                batch = self.batch_queue.force_flush()
                emit('new_message_batch', batch, broadcast=True)
                print(f'📢 System message broadcasted: {message}')
        except Exception as e:
            print(f'🚨 Error broadcasting system message: {str(e)}')

    def get_connected_user_count(self) -> int:
        """Get number of currently connected users"""
        return len(self.connected_users)
