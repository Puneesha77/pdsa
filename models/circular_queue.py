from flask_socketio import emit
from flask import request
from utils.message_utils import validate_message, validate_username, format_message_for_client
from utils.spam_utils import SpamDetector
from typing import Dict
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()


class ChatSocketHandlers:
    """
    Handles all WebSocket events for the chat system
    """

    SPAM_LIMIT = 5  # Number of spam messages before user is temporarily muted

    def __init__(self, message_system, socketio):
        """
        Initialize socket handlers
        """
        self.message_system = message_system
        self.socketio = socketio
        self.connected_users = {}  # Track connected users and spam counts
        self.spam_detector = SpamDetector()  # Spam detector

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

        @self.socketio.on('get_history')
        def handle_get_history():
            return self.on_get_history()

        @self.socketio.on_error_default
        def handle_error(e):
            return self.on_error(e)

    def on_connect(self):
        """Handle new connection with access code auth"""
        user_id = request.sid
        access_code = request.args.get("code")  # query param from client

        if access_code != os.getenv("CHAT_ACCESS_CODE"):
            print(f"❌ Unauthorized connection attempt: {user_id}")
            emit("error", {"type": "auth_error", "message": "Invalid access code"})
            return False  # reject connection

        self.connected_users[user_id] = {"spam_count": 0, "username": None}
        print(f"✅ Authorized user connected: {user_id}")

        return True

    def on_disconnect(self):
        """Handle user disconnection"""
        user_id = request.sid
        if user_id in self.connected_users:
            del self.connected_users[user_id]
        print('❌ User disconnected from Priority Chat')
        return True

    def on_send_message(self, data: Dict):
        """Handle incoming message from user"""
        user_id = request.sid

        try:
            raw_message = data.get('message', '')
            raw_user = data.get('user', 'Anonymous')
            manual_priority = data.get('priority', None)

            # Validate message
            message_validation = validate_message(raw_message)
            if not message_validation['valid']:
                emit('error', {'type': 'validation_error', 'message': message_validation['error']})
                return False

            # Validate username
            user_validation = validate_username(raw_user)
            clean_user = user_validation['sanitized_username']
            clean_message = message_validation['sanitized_message']

            # Save username for connected user
            if user_id in self.connected_users:
                self.connected_users[user_id]["username"] = clean_user

            # Run spam detection
            is_spam = self.spam_detector.is_spam(clean_message)
            if is_spam:
                self.connected_users[user_id]["spam_count"] += 1

                # Private alert for the sender
                emit('spam_alert', {
                    'message': 'Your message was flagged as spam and will be sent with lowest priority.'
                }, to=user_id)

                # Public alert for all other users
                emit('spam_alert', {
                    'user': clean_user,
                    'message': f'⚠️ {clean_user} sent a message flagged as spam!',
                    'highlight': True
                }, broadcast=True, include_self=False)

            # Check spam limit
            if self.connected_users[user_id]["spam_count"] > self.SPAM_LIMIT:
                emit('error', {
                    'type': 'spam_limit',
                    'message': 'You are temporarily muted for sending too many spam messages.'
                })
                return False

            # Convert manual priority to int if provided
            if manual_priority is not None:
                try:
                    manual_priority = int(manual_priority)
                    if manual_priority not in [1, 2, 3, 4]:
                        manual_priority = None
                except (ValueError, TypeError):
                    manual_priority = None

            # If spam, force priority = 4 (lowest)
            effective_priority = 4 if is_spam else manual_priority

            # Add message to priority queue
            message_obj = self.message_system.add_message(
                clean_message,
                clean_user,
                effective_priority
            )
            message_obj["is_spam"] = is_spam

            # Broadcast next message
            next_message = self.message_system.get_next_message()
            if next_message:
                formatted_msg = format_message_for_client(next_message)
                formatted_msg["is_spam"] = next_message.get("is_spam", False)

                # Highlight spam messages for all users
                if formatted_msg["is_spam"]:
                    formatted_msg["priority_name"] = "SPAM"
                    formatted_msg["priority"] = 4
                    formatted_msg["highlighted_spam"] = True
                    formatted_msg["alert_message"] = f'⚠️ {next_message["user"]} sent a spam message!'

                emit('new_message', formatted_msg, broadcast=True)

            return True

        except Exception as e:
            print(f'🚨 Error processing message: {str(e)}')
            emit('error', {'type': 'processing_error', 'message': 'Failed to process message'})
            return False

    def on_get_stats(self):
        """Handle request for queue statistics"""
        try:
            stats = self.message_system.get_queue_stats()
            emit('queue_stats', stats)
        except Exception as e:
            emit('error', {'type': 'stats_error', 'message': 'Failed to get statistics'})

    def on_get_history(self):
        """Handle request for chat history (last N messages)"""
        try:
            history = self.message_system.get_history()
            formatted_history = [format_message_for_client(msg) for msg in history]
            emit('chat_history', formatted_history)
        except Exception as e:
            emit('error', {'type': 'history_error', 'message': 'Failed to get chat history'})

    def on_error(self, e):
        """Handle socket errors"""
        print(f'🚨 Socket Error: {str(e)}')
        return True

    def broadcast_system_message(self, message: str, priority: int = 3):
        """Broadcast a system message to all users with spam check"""
        try:
            is_spam = self.spam_detector.is_spam(message)
            effective_priority = 4 if is_spam else priority

            system_msg = self.message_system.add_message(
                message, "SYSTEM", effective_priority
            )

            next_message = self.message_system.get_next_message()
            if next_message:
                formatted_msg = format_message_for_client(next_message)
                formatted_msg["is_spam"] = is_spam
                if is_spam:
                    formatted_msg["priority_name"] = "SPAM"
                    formatted_msg["priority"] = 4
                    formatted_msg["highlighted_spam"] = True
                    formatted_msg["alert_message"] = f'⚠️ SYSTEM message flagged as spam!'

                emit('new_message', formatted_msg, broadcast=True)

        except Exception as e:
            print(f'🚨 Error broadcasting system message: {str(e)}')

    def get_connected_user_count(self) -> int:
        """Get number of currently connected users"""
        return len(self.connected_users)
