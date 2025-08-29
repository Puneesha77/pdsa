"""
User Management System for Chat
Handles user registration, authentication, and session management
"""

import time
import hashlib
import secrets
from typing import Dict, List, Optional, Set
from collections import defaultdict


class User:
    """
    Represents a chat user
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
    
    def to_dict(self) -> Dict:
        """Convert user to dictionary for JSON serialization"""
        return {
            'username': self.username,
            'created_at': self.created_at,
            'last_active': self.last_active,
            'is_online': self.is_online,
            'message_count': self.message_count,
            'join_time': self.join_time
        }


class UserManager:
    """
    Manages all user operations including authentication and sessions
    """
    
    def __init__(self):
        self.users: Dict[str, User] = {}  # username -> User object
        self.sessions: Dict[str, str] = {}  # session_id -> username
        self.online_users: Set[str] = set()  # Set of online usernames
        self.socket_to_user: Dict[str, str] = {}  # socket_id -> username
        self.failed_login_attempts = defaultdict(int)  # Track failed logins
        
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
        Mark a user as online
        
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
        return True
    
    def set_user_offline(self, socket_id: str) -> Optional[str]:
        """
        Mark a user as offline by socket ID
        
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
        
        self.online_users.discard(username)
        del self.socket_to_user[socket_id]
        
        print(f"🔴 {username} went offline")
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
            online_list.append({
                'username': username,
                'join_time': user.join_time,
                'message_count': user.message_count,
                'last_active': user.last_active
            })
        
        # Sort by join time (earliest first)
        online_list.sort(key=lambda x: x['join_time'] or 0)
        return online_list
    
    def get_user_stats(self) -> Dict:
        """
        Get overall user statistics
        
        Returns:
            Dict with user statistics
        """
        return {
            'total_registered': len(self.users),
            'currently_online': len(self.online_users),
            'total_failed_logins': sum(self.failed_login_attempts.values()),
            'online_users': list(self.online_users)
        }
    
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