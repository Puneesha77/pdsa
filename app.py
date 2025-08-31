"""
Priority Chat System - Flask Backend
Real-time chat with priority queues and spam detection
"""

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
import logging
from dotenv import load_dotenv
import os
import re
from datetime import datetime
from collections import defaultdict
from collections import deque
from models.circular_queue import CircularQueue

# Load environment variables
load_dotenv()
#test

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

class PriorityMessageQueue:
    """Priority-based message queue system"""
    
    def __init__(self, max_messages_per_category=10):
        self.max_messages_per_category = max_messages_per_category
        
        # Separate queues for each priority level
        self.queues = {
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
    
    def add_message(self, message_data):
        """Add message to appropriate priority queue"""
        priority = message_data.get("priority", 3)
        
        # Add unique message ID
        self.message_counter += 1
        message_data["id"] = self.message_counter
        
        # Add to appropriate queue
        self.queues[priority].append(message_data)
        
        print(f"📝 Added message to {self.priority_names[priority]} queue: '{message_data['text'][:30]}...'")
        
        return message_data
    
    def get_all_messages_organized(self):
        """Get all messages organized by priority"""
        organized_messages = {
            "urgent": list(self.queues[1]),
            "high": list(self.queues[2]), 
            "normal": list(self.queues[3]),
            "low": list(self.queues[4])
        }
        
        return organized_messages
    
    def get_queue_stats(self):
        """Get statistics about message queues"""
        return {
            "urgent_count": len(self.queues[1]),
            "high_count": len(self.queues[2]),
            "normal_count": len(self.queues[3]),
            "low_count": len(self.queues[4]),
            "total_messages": sum(len(queue) for queue in self.queues.values())
        }

def detect_message_priority(message: str, manual_priority: int = None) -> int:
    """
    Auto-detect message priority based on content analysis
    
    Args:
        message: The message text to analyze
        manual_priority: Manual priority override (1-4)
        
    Returns:
        Priority level (1=URGENT, 2=HIGH, 3=NORMAL, 4=LOW)
    """
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
    if caps_ratio > 0.7 and len(message) > 10:  # More than 70% caps and reasonably long
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
message_queue = PriorityMessageQueue()

# Get access code from environment
ACCESS_CODE = os.getenv("CHAT_ACCESS_CODE", "supersecret123")

# Track connected users
connected_users = {}  # session_id -> username

@socketio.on("connect")
def handle_connect():
    print(f"Client connected: {request.sid}")
    
    # Send current message organization to new client
    organized_messages = message_queue.get_all_messages_organized()
    emit("message_organization", organized_messages)

@socketio.on("disconnect")
def handle_disconnect():
    if request.sid in connected_users:
        username = connected_users[request.sid]
        print(f"User '{username}' disconnected: {request.sid}")
        del connected_users[request.sid]
    else:
        print(f"Client disconnected: {request.sid}")

@socketio.on("login")
def handle_login(data):
    """Handle user login with secret key"""
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
        
        # Check if username already taken
        if username in connected_users.values():
            emit("login_error", "Username already taken")
            return
        
        # Success - store user
        connected_users[request.sid] = username
        emit("login_success", {"username": username})
        print(f"User '{username}' logged in successfully")
        
        # Send current message organization to logged in user
        organized_messages = message_queue.get_all_messages_organized()
        emit("message_organization", organized_messages)
        
        # Send welcome message
        #welcome_msg = {
           # "user": "System",
            #"text": f"Welcome {username}! Messages are organized by priority.",
           # "priority": 3,
           # "priority_name": "NORMAL",
           # "is_spam": False,
            #"alert_message": None,
            #"timestamp": datetime.now().isoformat()
        #}
        
        # Add welcome message to queue and broadcast organization
        # message_queue.add_message(welcome_msg)
        organized_messages = message_queue.get_all_messages_organized()
        emit("message_organization", organized_messages, broadcast=True)
        
    except Exception as e:
        print(f"Login error: {e}")
        emit("login_error", "Login failed")

@socketio.on("send_message")
def handle_message(data):
    """Handle incoming messages with priority detection and spam checking"""
    try:
        if request.sid not in connected_users:
            print(f"Message from unauthorized user: {request.sid}")
            return
        
        user = connected_users[request.sid]
        message = data.get("message", "").strip()
        manual_priority = data.get("priority")  # Manual priority from user
        
        if not message:
            return
        
        print(f"Message from '{user}': '{message}'")
        
        # Convert manual priority to int if provided
        try:
            manual_priority = int(manual_priority) if manual_priority is not None else None
            if manual_priority is not None and manual_priority not in [1, 2, 3, 4]:
                manual_priority = None
        except (ValueError, TypeError):
            manual_priority = None
        
        # Auto-detect priority (with manual override support)
        priority = detect_message_priority(message, manual_priority)
        
        # Check for spam
        is_spam = spam_detector.is_spam(message)
        
        # If spam detected, force priority to 4 (LOW)
        if is_spam:
            priority = 4
            alert_message = "SPAM DETECTED"
            print(f"SPAM MESSAGE from '{user}': '{message}'")
        else:
            alert_message = None
        
        # Priority names mapping
        priority_names = {
            1: "URGENT",
            2: "HIGH", 
            3: "NORMAL",
            4: "LOW"
        }
        
        msg_data = {
            "user": user,
            "text": message,
            "priority": priority,
            "priority_name": priority_names.get(priority, "NORMAL"),
            "is_spam": is_spam,
            "alert_message": alert_message,
            "timestamp": datetime.now().isoformat()
        }
        
        # Add message to priority queue
        message_queue.add_message(msg_data)
        
        # Get organized messages and broadcast to all clients
        organized_messages = message_queue.get_all_messages_organized()
        emit("message_organization", organized_messages, broadcast=True)
        
        print(f"📤 Broadcasted organized messages: Priority={priority_names.get(priority)}, Spam={is_spam}")
        
    except Exception as e:
        print(f"Message handling error: {e}")

@socketio.on("request_messages")
def handle_request_messages():
    """Send current message organization to requesting client"""
    organized_messages = message_queue.get_all_messages_organized()
    emit("message_organization", organized_messages)

@app.route('/')
def index():
    """Serve the chat interface"""
    return render_template('index.html')

@app.route('/health')
def health_check():
    """Health check endpoint"""
    queue_stats = message_queue.get_queue_stats()
    return {
        'status': 'healthy',
        'system': 'Priority Chat System',
        'version': '1.0.0',
        'connected_users': len(connected_users),
        'queue_stats': queue_stats
    }

@app.route('/stats')
def get_stats():
    """Get system statistics with message history count"""
    queue_stats = message_queue.get_queue_stats()
    history_count = len(message_queue.get_all_messages_organized())  # just a number ✅
    
    return {
        'connected_users': len(connected_users),
        'access_code_configured': bool(ACCESS_CODE),
        'spam_detection': 'active',
        'message_queues': queue_stats,
        'message_history_count': history_count,  # replaced history list with count
        'uptime': 'running'
    }

@app.route('/messages')
def get_messages():
    """API endpoint to get organized messages"""
    organized_messages = message_queue.get_all_messages_organized()
    return organized_messages

def main():
    """Main application entry point"""
    print("=" * 50)
    print("🚀 Priority Queue Chat System Starting...")
    print("=" * 50)
    print("📍 Main Interface: http://localhost:5000")
    print("📊 Health Check:  http://localhost:5000/health")
    print("📈 Statistics:    http://localhost:5000/stats")
    print("💬 Messages API:  http://localhost:5000/messages")
    print("=" * 50)
    print("🎯 Priority Queue Features:")
    print("  🔴 URGENT Messages   (Top Priority)")
    print("  🟡 HIGH Messages     (Second Priority)")
    print("  🟢 NORMAL Messages   (Third Priority)")
    print("  ⚫ LOW/SPAM Messages (Bottom Priority)")
    print("=" * 50)
    print("✨ Auto-Detection:")
    print("  ✅ Keywords: 'urgent', 'emergency', 'help', 'error', etc.")
    print("  ✅ ALL CAPS messages (>70% capitals)")
    print("  ✅ @mentions for HIGH priority")
    print("  ✅ Spam detection forces LOW priority")
    print("  ✅ Manual priority override available")
    print("=" * 50)
    print(f"🔑 Access Code: {ACCESS_CODE}")
    print("=" * 50)
    
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
    except Exception as e:
        print(f"🚨 Fatal error: {str(e)}")

if __name__ == '__main__':
    main()