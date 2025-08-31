#!/usr/bin/env python3
"""
Priority Chat System Backend Server
Handles Socket.IO connections and spam detection
"""

import re
import socketio
import eventlet
from flask import Flask
from datetime import datetime

class SpamDetector:
    """
    Enhanced Spam Detector for chat messages.
    Detects spam using keywords, normalization, repetition, and URL patterns.
    """
    
    def __init__(self):
        # Common spam keywords/phrases
        self.spam_keywords = [
            "buy now", "free money", "visit this site",
            "click here", "subscribe", "lottery",
            "win cash", "make money fast", "100% free",
            "limited offer", "act now", "double your",
            "work from home", "earn $", "make $$$",
            "guaranteed", "no risk", "urgent", "congratulations",
            "winner", "claim now", "exclusive deal"
        ]
        
        # Regex patterns
        self.repeated_char_pattern = re.compile(r"(.)\1{4,}")  # e.g. "heyyyyy"
        self.url_pattern = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)
        self.repeated_word_pattern = re.compile(r"\b(\w+)\s+\1\s+\1", re.IGNORECASE)  # word repeated 3+ times
        self.caps_pattern = re.compile(r"[A-Z]{10,}")  # Too many capitals
    
    def normalize(self, text: str) -> str:
        """Remove punctuation/symbols and lowercase the text."""
        return re.sub(r"[^a-z0-9\s]", "", text.lower())
    
    def is_spam(self, text: str) -> bool:
        """
        Check if message is spam.
        Args:
            text: message string
        Returns:
            bool: True if spam detected
        """
        if not text:
            return False
        
        text_lower = text.lower()
        text_clean = self.normalize(text)
        
        # 1. Keyword-based detection
        for keyword in self.spam_keywords:
            if keyword in text_clean:
                print(f"SPAM DETECTED: Keyword '{keyword}' found in message: '{text[:50]}...'")
                return True
        
        # 2. Repeated character spam (e.g., "heyyyyy")
        if self.repeated_char_pattern.search(text_lower):
            print(f"SPAM DETECTED: Repeated characters in message: '{text[:50]}...'")
            return True
        
        # 3. Repeated word spam (e.g., "free free free")
        if self.repeated_word_pattern.search(text_lower):
            print(f"SPAM DETECTED: Repeated words in message: '{text[:50]}...'")
            return True
        
        # 4. URL / suspicious links
        if self.url_pattern.search(text):
            print(f"SPAM DETECTED: URL found in message: '{text[:50]}...'")
            return True
        
        # 5. Excessive message length
        if len(text) > 300:
            print(f"SPAM DETECTED: Message too long ({len(text)} chars): '{text[:50]}...'")
            return True
        
        # 6. Too many capital letters
        if self.caps_pattern.search(text):
            print(f"SPAM DETECTED: Too many capitals in message: '{text[:50]}...'")
            return True
        
        return False

class ChatServer:
    """Main chat server with spam detection"""
    
    def __init__(self):
        # Initialize Flask app and Socket.IO
        self.app = Flask(__name__)
        self.sio = socketio.Server(cors_allowed_origins="*")
        self.app.wsgi_app = socketio.WSGIApp(self.sio, self.app.wsgi_app)
        
        # Initialize spam detector
        self.spam_detector = SpamDetector()
        
        # Simple authentication (in production, use proper auth)
        self.valid_secrets = ["admin123", "user456", "test789"]
        
        # Connected users
        self.connected_users = {}
        
        # Priority mapping
        self.priority_names = {
            1: "URGENT",
            2: "HIGH", 
            3: "NORMAL",
            4: "LOW"
        }
        
        # Setup event handlers
        self.setup_handlers()
    
    def setup_handlers(self):
        """Setup Socket.IO event handlers"""
        
        @self.sio.event
        def connect(sid, environ):
            print(f"Client connected: {sid}")
        
        @self.sio.event
        def disconnect(sid):
            if sid in self.connected_users:
                username = self.connected_users[sid]
                print(f"User '{username}' disconnected: {sid}")
                del self.connected_users[sid]
            else:
                print(f"Client disconnected: {sid}")
        
        @self.sio.event
        def login(sid, data):
            """Handle user login"""
            try:
                username = data.get('username', '').strip()
                secret_key = data.get('secretKey', '').strip()
                
                if not username or not secret_key:
                    self.sio.emit('login_error', 'Username and secret key required', room=sid)
                    return
                
                if secret_key not in self.valid_secrets:
                    self.sio.emit('login_error', 'Invalid secret key', room=sid)
                    return
                
                # Check if username already taken
                if username in self.connected_users.values():
                    self.sio.emit('login_error', 'Username already taken', room=sid)
                    return
                
                # Success - store user
                self.connected_users[sid] = username
                self.sio.emit('login_success', {'username': username}, room=sid)
                
                print(f"User '{username}' logged in successfully: {sid}")
                
                # Send welcome message
                welcome_msg = {
                    'user': 'System',
                    'text': f"Welcome {username}!",
                    'priority': 3,
                    'priority_name': 'NORMAL',
                    'is_spam': False,
                    'alert_message': None,
                    'timestamp': datetime.now().isoformat()
                }
                self.sio.emit('new_message', welcome_msg, room=sid)
                
            except Exception as e:
                print(f"Login error: {e}")
                self.sio.emit('login_error', 'Login failed', room=sid)
        
        @self.sio.event
        def send_message(sid, data):
            """Handle incoming messages with spam detection"""
            try:
                if sid not in self.connected_users:
                    return
                
                username = self.connected_users[sid]
                message_text = data.get('message', '').strip()
                user_priority = data.get('priority')
                
                if not message_text:
                    return
                
                print(f"Message from '{username}': '{message_text}'")
                
                # Detect spam
                is_spam = self.spam_detector.is_spam(message_text)
                
                # If spam detected, override priority to LOW (4)
                if is_spam:
                    final_priority = 4
                    alert_message = "DETECTED"
                    print(f"SPAM MESSAGE from '{username}': '{message_text}'")
                else:
                    final_priority = user_priority if user_priority is not None else 3
                    alert_message = None
                
                # Get priority name
                priority_name = self.priority_names.get(final_priority, "NORMAL")
                
                # Create message object
                message_obj = {
                    'user': username,
                    'text': message_text,
                    'priority': final_priority,
                    'priority_name': priority_name,
                    'is_spam': is_spam,
                    'alert_message': alert_message,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Broadcast to all connected clients
                self.sio.emit('new_message', message_obj)
                
                print(f"Broadcasted message: Priority={priority_name}, Spam={is_spam}")
                
            except Exception as e:
                print(f"Message error: {e}")
    
    def run(self, host='0.0.0.0', port=5000, debug=True):
        """Start the server"""
        print(f"Starting Priority Chat Server on {host}:{port}")
        print(f"Valid secret keys: {', '.join(self.valid_secrets)}")
        print("Spam detection is ACTIVE")
        
        if debug:
            eventlet.wsgi.server(eventlet.listen((host, port)), self.app, debug=True)
        else:
            eventlet.wsgi.server(eventlet.listen((host, port)), self.app)

def main():
    """Main entry point"""
    server = ChatServer()
    
    try:
        server.run(host='0.0.0.0', port=5000, debug=True)
    except KeyboardInterrupt:
        print("\nServer shutting down...")
    except Exception as e:
        print(f"Server error: {e}")

if __name__ == "__main__":
    main()