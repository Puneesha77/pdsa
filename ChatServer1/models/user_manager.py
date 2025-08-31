"""
User Management System for Chat with Offline Queue Integration
Handles user registration, authentication, session management, and offline queue coordination
UPDATED: Added offline queue integration hooks
"""

import time
import hashlib
import secrets
from typing import Dict, List, Optional, Set
from collections import defaultdict


class User:
    """
    Represents a chat user with offline queue support
    """
    def __init__(self, username: str, password_hash: str, session_id: str):
        self.username = username
        self.password_hash = password_hash
        self.session_id = session_id
        self.created_at = time.time()
        self.last_active = time.time()
        self.is_online = False
        self.socket_id = None
        self.message_count = 0
        self.join_time = None
        # NEW: Offline-related fields
        self.last_offline_time = None
        self.offline_message_count = 0
        self.total_offline_messages_received = 0
    
    def to_dict(self) -> Dict:
        """Convert user to dictionary for JSON serialization"""
        return {
            'username': self.username,
            'created_at': self.created_at,
            'last_active': self.last_active,
            'is_online': self.is_online,
            'message_count': self.message_count,
            'join_time': self.join_time,
            'last_offline_time': self.last_offline_time,  # NEW
            'total_offline_messages_received': self.total_offline_messages_received  # NEW
        }


class UserManager:
    """
    Manages all user operations including authentication, sessions, and offline queue coordination
    UPDATED: Added offline queue integration methods
    """
    
    def __init__(self):
        self.users: Dict[str, User] = {}  # username -> User object
        self.sessions: Dict[str, str] = {}  # session_id -> username
        self.online_users: Set[str] = set()  # Set of online usernames
        self.socket_to_user: Dict[str, str] = {}  # socket_id -> username
        self.failed_login_attempts = defaultdict(int)  # Track failed logins
        
        # NEW: Offline queue reference (will be set by main app)
        self.offline_queue = None
        
    def set_offline_queue(self, offline_queue):
        """
        Set reference to offline queue for integration
        
        Args:
            offline_queue: OfflineQueue instance
        """
        self.offline_queue = offline_queue
        print("🔗 UserManager linked with OfflineQueue")
        
    def hash_password(self, password: str) -> str:
        """
        Hash a password securely
        
        Args:
            password: Plain text password
            
        Returns:
            Hashed password string
        """
        # Add salt for security
        salt = "chat_system_salt_2025"
        return hashlib.sha256((password + salt).encode()).hexdigest()
    
    def generate_session_id(self) -> str:
        """
        Generate a secure session ID
        
        Returns:
            Random session ID string
        """
        return secrets.token_urlsafe(32)
    
    def register_user(self, username: str, password: str) -> Dict:
        """
        Register a new user
        
        Args:
            username: Desired username
            password: User password
            
        Returns:
            Dict with registration result
        """
        # Validate username
        username = username.strip()
        
        if not username:
            return {'success': False, 'error': 'Username cannot be empty'}
        
        if len(username) < 3:
            return {'success': False, 'error': 'Username must be at least 3 characters'}
        
        if len(username) > 20:
            return {'success': False, 'error': 'Username must be less than 20 characters'}
        
        # Check if username already exists (case insensitive)
        if username.lower() in [u.lower() for u in self.users.keys()]:
            return {'success': False, 'error': 'Username already taken'}
        
        # Validate password
        if len(password) < 4:
            return {'success': False, 'error': 'Password must be at least 4 characters'}
        
        # Create new user
        session_id = self.generate_session_id()
        password_hash = self.hash_password(password)
        
        user = User(username, password_hash, session_id)
        
        self.users[username] = user
        self.sessions[session_id] = username
        
        print(f"👤 New user registered: {username}")
        
        return {
            'success': True,
            'username': username,
            'session_id': session_id,
            'message': f'Welcome to Priority Chat, {username}!'
        }
    
    def login_user(self, username: str, password: str) -> Dict:
        """
        Authenticate a user login
        
        Args:
            username: Username
            password: Password
            
        Returns:
            Dict with login result
        """
        # Check if user exists
        if username not in self.users:
            self.failed_login_attempts[username] += 1
            return {'success': False, 'error': 'Invalid username or password'}
        
        user = self.users[username]
        password_hash = self.hash_password(password)
        
        # Verify password
        if user.password_hash != password_hash:
            self.failed_login_attempts[username] += 1
            return {'success': False, 'error': 'Invalid username or password'}
        
        # Generate new session
        session_id = self.generate_session_id()
        user.session_id = session_id
        self.sessions[session_id] = username
        
        # Reset failed attempts
        self.failed_login_attempts[username] = 0
        
        print(f"🔑 User logged in: {username}")
        
        return {
            'success': True,
            'username': username,
            'session_id': session_id,
            'message': f'Welcome back, {username}!'
        }
    
    def validate_session(self, session_id: str) -> Optional[str]:
        """
        Check if a session ID is valid
        
        Args:
            session_id: Session ID to validate
            
        Returns:
            Username if valid, None if invalid
        """
        return self.sessions.get(session_id)
    
    def set_user_online(self, username: str, socket_id: str) -> bool:
        """
        Mark a user as online and handle offline message delivery
        UPDATED: Now integrates with offline queue
        
        Args:
            username: Username
            socket_id: Socket connection ID
            
        Returns:
            True if successful, False if user not found
        """
        if username not in self.users:
            return False
        
        user = self.users[username]
        user.is_online = True
        user.socket_id = socket_id
        user.join_time = time.time()
        user.last_active = time.time()
        
        self.online_users.add(username)
        self.socket_to_user[socket_id] = username
        
        print(f"🟢 {username} is now online")
        
        # NEW: Handle offline messages when user comes online
        if self.offline_queue:
            try:
                delivery_summary = self.offline_queue.handle_user_online(username, self)
                if delivery_summary and delivery_summary['messages_delivered'] > 0:
                    user.total_offline_messages_received += delivery_summary['messages_delivered']
                    print(f"📬 Delivered {delivery_summary['messages_delivered']} offline messages to {username}")
            except Exception as e:
                print(f"❌ Error handling offline messages for {username}: {e}")
        
        return True
    
    def set_user_offline(self, socket_id: str) -> Optional[str]:
        """
        Mark a user as offline by socket ID and prepare for offline message storage
        UPDATED: Now integrates with offline queue
        
        Args:
            socket_id: Socket connection ID
            
        Returns:
            Username that went offline, or None
        """
        username = self.socket_to_user.get(socket_id)
        if not username:
            return None
        
        user = self.users[username]
        user.is_online = False
        user.socket_id = None
        user.last_offline_time = time.time()  # NEW: Track when user went offline
        
        self.online_users.discard(username)
        del self.socket_to_user[socket_id]
        
        print(f"🔴 {username} went offline")
        
        # NEW: Notify offline queue that user went offline
        if self.offline_queue:
            try:
                self.offline_queue.handle_user_offline(username)
            except Exception as e:
                print(f"❌ Error handling user offline for {username}: {e}")
        
        return username
    
    def get_user_by_socket(self, socket_id: str) -> Optional[str]:
        """
        Get username by socket ID
        
        Args:
            socket_id: Socket connection ID
            
        Returns:
            Username or None
        """
        return self.socket_to_user.get(socket_id)
    
    def update_user_activity(self, username: str):
        """
        Update user's last active timestamp
        
        Args:
            username: Username to update
        """
        if username in self.users:
            self.users[username].last_active = time.time()
            self.users[username].message_count += 1
    
    def get_online_users(self) -> List[Dict]:
        """
        Get list of currently online users
        
        Returns:
            List of online user info dicts
        """
        online_list = []
        for username in self.online_users:
            user = self.users[username]
            
            # NEW: Include offline message count
            offline_msg_count = 0
            if self.offline_queue:
                offline_msg_count = self.offline_queue.get_offline_message_count(username)
            
            online_list.append({
                'username': username,
                'join_time': user.join_time,
                'message_count': user.message_count,
                'last_active': user.last_active,
                'offline_messages_waiting': offline_msg_count,  # NEW
                'total_offline_received': user.total_offline_messages_received  # NEW
            })
        
        # Sort by join time (earliest first)
        online_list.sort(key=lambda x: x['join_time'] or 0)
        return online_list
    
    def get_offline_users(self) -> List[Dict]:
        """
        NEW: Get list of currently offline users
        
        Returns:
            List of offline user info dicts
        """
        offline_list = []
        
        for username, user in self.users.items():
            if not user.is_online:
                # Get offline message count
                offline_msg_count = 0
                if self.offline_queue:
                    offline_msg_count = self.offline_queue.get_offline_message_count(username)
                
                offline_list.append({
                    'username': username,
                    'last_active': user.last_active,
                    'last_offline_time': user.last_offline_time,
                    'offline_duration': time.time() - (user.last_offline_time or user.last_active),
                    'offline_messages_waiting': offline_msg_count,
                    'total_messages_sent': user.message_count,
                    'total_offline_received': user.total_offline_messages_received
                })
        
        # Sort by offline duration (longest offline first)
        offline_list.sort(key=lambda x: x['offline_duration'], reverse=True)
        return offline_list
    
    def get_user_stats(self) -> Dict:
        """
        Get overall user statistics with offline queue integration
        UPDATED: Now includes offline-related statistics
        
        Returns:
            Dict with user statistics
        """
        offline_users_with_messages = 0
        total_offline_messages = 0
        
        if self.offline_queue:
            offline_status = self.offline_queue.get_queue_status()
            offline_users_with_messages = offline_status['users_with_messages']
            total_offline_messages = offline_status['total_pending_messages']
        
        return {
            'total_registered': len(self.users),
            'currently_online': len(self.online_users),
            'currently_offline': len(self.users) - len(self.online_users),
            'total_failed_logins': sum(self.failed_login_attempts.values()),
            'online_users': list(self.online_users),
            'offline_users_with_messages': offline_users_with_messages,  # NEW
            'total_offline_messages_pending': total_offline_messages,  # NEW
            'offline_queue_active': self.offline_queue is not None  # NEW
        }
    
    def get_user_offline_summary(self, username: str) -> Dict:
        """
        NEW: Get comprehensive offline summary for a specific user
        
        Args:
            username: Username to get summary for
            
        Returns:
            Dict with offline summary information
        """
        if username not in self.users:
            return {'error': 'User not found'}
        
        user = self.users[username]
        
        # Get offline message count
        offline_msg_count = 0
        if self.offline_queue:
            offline_msg_count = self.offline_queue.get_offline_message_count(username)
        
        summary = {
            'username': username,
            'is_online': user.is_online,
            'last_active': user.last_active,
            'last_offline_time': user.last_offline_time,
            'current_offline_messages': offline_msg_count,
            'total_offline_messages_received': user.total_offline_messages_received,
            'created_at': user.created_at,
            'total_messages_sent': user.message_count
        }
        
        if not user.is_online and user.last_offline_time:
            summary['offline_duration'] = time.time() - user.last_offline_time
        
        return summary
    
    def is_username_available(self, username: str) -> bool:
        """
        Check if a username is available for registration
        
        Args:
            username: Username to check
            
        Returns:
            True if available, False if taken
        """
        return username.lower() not in [u.lower() for u in self.users.keys()]
    
    def logout_user(self, session_id: str) -> bool:
        """
        Log out a user by session ID
        UPDATED: Now handles offline queue coordination
        
        Args:
            session_id: Session ID to logout
            
        Returns:
            True if successful, False if session not found
        """
        username = self.sessions.get(session_id)
        if not username:
            return False
        
        # Remove session
        del self.sessions[session_id]
        
        # Set user offline if they were online
        user = self.users[username]
        if user.socket_id:
            self.set_user_offline(user.socket_id)
        
        print(f"👋 {username} logged out")
        return True
    
    def get_users_for_broadcast(self, exclude_user: Optional[str] = None) -> Dict:
        """
        NEW: Get online and offline users for message broadcasting
        
        Args:
            exclude_user: Username to exclude from broadcast
            
        Returns:
            Dict with online and offline user lists
        """
        online_users = []
        offline_users = []
        
        for username, user in self.users.items():
            if exclude_user and username == exclude_user:
                continue
                
            if user.is_online:
                online_users.append({
                    'username': username,
                    'socket_id': user.socket_id,
                    'last_active': user.last_active
                })
            else:
                offline_users.append({
                    'username': username,
                    'last_offline': user.last_offline_time or user.last_active,
                    'offline_duration': time.time() - (user.last_offline_time or user.last_active)
                })
        
        return {
            'online_users': online_users,
            'offline_users': offline_users,
            'total_online': len(online_users),
            'total_offline': len(offline_users)
        }
    
    def cleanup_inactive_sessions(self, max_inactive_hours: int = 24) -> int:
        """
        NEW: Clean up sessions that have been inactive for too long
        
        Args:
            max_inactive_hours: Maximum hours of inactivity before cleanup
            
        Returns:
            int: Number of sessions cleaned up
        """
        current_time = time.time()
        max_inactive_seconds = max_inactive_hours * 3600
        
        inactive_sessions = []
        
        for session_id, username in self.sessions.items():
            user = self.users.get(username)
            if user:
                inactive_time = current_time - user.last_active
                if inactive_time > max_inactive_seconds and not user.is_online:
                    inactive_sessions.append(session_id)
        
        # Remove inactive sessions
        for session_id in inactive_sessions:
            username = self.sessions[session_id]
            del self.sessions[session_id]
            print(f"🧹 Cleaned up inactive session for {username}")
        
        return len(inactive_sessions)
    
    def force_user_offline(self, username: str) -> bool:
        """
        NEW: Force a user offline (admin function)
        
        Args:
            username: Username to force offline
            
        Returns:
            bool: True if user was online and forced offline
        """
        if username not in self.users or not self.users[username].is_online:
            return False
        
        user = self.users[username]
        socket_id = user.socket_id
        
        if socket_id:
            self.set_user_offline(socket_id)
            print(f"👮 Forced {username} offline")
            return True
        
        return False
    
    def get_user_activity_report(self) -> Dict:
        """
        NEW: Get detailed user activity report
        
        Returns:
            Dict with comprehensive user activity data
        """
        current_time = time.time()
        
        activity_report = {
            'report_timestamp': current_time,
            'summary': {
                'total_users': len(self.users),
                'online_users': len(self.online_users),
                'offline_users': len(self.users) - len(self.online_users),
                'users_with_offline_messages': 0,
                'total_offline_messages': 0
            },
            'user_details': []
        }
        
        # Get offline queue stats if available
        if self.offline_queue:
            offline_status = self.offline_queue.get_queue_status()
            activity_report['summary']['users_with_offline_messages'] = offline_status['users_with_messages']
            activity_report['summary']['total_offline_messages'] = offline_status['total_pending_messages']
        
        # Add individual user details
        for username, user in self.users.items():
            offline_msg_count = 0
            if self.offline_queue:
                offline_msg_count = self.offline_queue.get_offline_message_count(username)
            
            user_detail = {
                'username': username,
                'is_online': user.is_online,
                'created_at': user.created_at,
                'last_active': user.last_active,
                'message_count': user.message_count,
                'offline_messages_waiting': offline_msg_count,
                'total_offline_received': user.total_offline_messages_received,
                'inactive_duration': current_time - user.last_active,
                'session_active': user.session_id in self.sessions
            }
            
            if user.last_offline_time:
                user_detail['last_offline_time'] = user.last_offline_time
                if not user.is_online:
                    user_detail['offline_duration'] = current_time - user.last_offline_time
            
            activity_report['user_details'].append(user_detail)
        
        # Sort by last active (most recent first)
        activity_report['user_details'].sort(key=lambda x: x['last_active'], reverse=True)
        
        return activity_report
    
    def update_offline_message_delivery_stats(self, username: str, delivered_count: int):
        """
        NEW: Update user stats when offline messages are delivered
        
        Args:
            username: Username that received messages
            delivered_count: Number of messages delivered
        """
        if username in self.users:
            self.users[username].total_offline_messages_received += delivered_count
            print(f"📊 Updated offline delivery stats for {username}: +{delivered_count} messages")
    
    def get_all_users_summary(self) -> List[Dict]:
        """
        NEW: Get summary of all users (online and offline)
        
        Returns:
            List[Dict]: Summary information for all users
        """
        current_time = time.time()
        users_summary = []
        
        for username, user in self.users.items():
            offline_msg_count = 0
            if self.offline_queue:
                offline_msg_count = self.offline_queue.get_offline_message_count(username)
            
            summary = {
                'username': username,
                'status': 'online' if user.is_online else 'offline',
                'last_active': user.last_active,
                'inactive_duration': current_time - user.last_active,
                'message_count': user.message_count,
                'offline_messages_waiting': offline_msg_count,
                'created_at': user.created_at,
                'account_age': current_time - user.created_at
            }
            
            if user.is_online:
                summary['join_time'] = user.join_time
                summary['session_duration'] = current_time - (user.join_time or current_time)
            else:
                summary['last_offline_time'] = user.last_offline_time
                if user.last_offline_time:
                    summary['offline_duration'] = current_time - user.last_offline_time
            
            users_summary.append(summary)
        
        return users_summary
    
    def check_user_exists(self, username: str) -> bool:
        """
        NEW: Check if a user exists in the system
        
        Args:
            username: Username to check
            
        Returns:
            bool: True if user exists
        """
        return username in self.users
    
    def get_user_info(self, username: str) -> Optional[Dict]:
        """
        NEW: Get detailed information about a specific user
        
        Args:
            username: Username to get info for
            
        Returns:
            Optional[Dict]: User information or None if not found
        """
        if username not in self.users:
            return None
        
        user = self.users[username]
        current_time = time.time()
        
        offline_msg_count = 0
        if self.offline_queue:
            offline_msg_count = self.offline_queue.get_offline_message_count(username)
        
        user_info = {
            'username': username,
            'is_online': user.is_online,
            'created_at': user.created_at,
            'last_active': user.last_active,
            'message_count': user.message_count,
            'session_id': user.session_id,
            'socket_id': user.socket_id,
            'account_age': current_time - user.created_at,
            'inactive_duration': current_time - user.last_active,
            'offline_messages_waiting': offline_msg_count,
            'total_offline_received': user.total_offline_messages_received
        }
        
        if user.is_online:
            user_info['join_time'] = user.join_time
            if user.join_time:
                user_info['session_duration'] = current_time - user.join_time
        else:
            user_info['last_offline_time'] = user.last_offline_time
            if user.last_offline_time:
                user_info['offline_duration'] = current_time - user.last_offline_time
        
        return user_info