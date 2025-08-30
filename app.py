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

# Initialize spam detector
spam_detector = SpamDetector()

# Get access code from environment
ACCESS_CODE = os.getenv("CHAT_ACCESS_CODE", "supersecret123")

# Track connected users
connected_users = {}  # session_id -> username

@socketio.on("connect")
def handle_connect():
    print(f"Client connected: {request.sid}")

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
        
        # Send welcome message
        welcome_msg = {
            "user": "System",
            "text": f"Welcome {username}!",
            "priority": 3,
            "priority_name": "NORMAL",
            "is_spam": False,
            "alert_message": None,
            "timestamp": datetime.now().isoformat()
        }
        emit("new_message", welcome_msg)
        
    except Exception as e:
        print(f"Login error: {e}")
        emit("login_error", "Login failed")

@socketio.on("send_message")
def handle_message(data):
    """Handle incoming messages with spam detection"""
    try:
        if request.sid not in connected_users:
            print(f"Message from unauthorized user: {request.sid}")
            return
        
        user = connected_users[request.sid]
        message = data.get("message", "").strip()
        priority = data.get("priority", 3)
        
        if not message:
            return
        
        print(f"Message from '{user}': '{message}'")
        
        # Convert priority to int
        try:
            priority = int(priority) if priority is not None else 3
            if priority not in [1, 2, 3, 4]:
                priority = 3
        except (ValueError, TypeError):
            priority = 3
        
        # Check for spam
        is_spam = spam_detector.is_spam(message)
        
        # If spam detected, force priority to 4 (LOW)
        if is_spam:
            priority = 4
            alert_message = "DETECTED"
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
        
        # Broadcast to all connected clients
        emit("new_message", msg_data, broadcast=True)
        print(f"Broadcasted message: Priority={priority_names.get(priority)}, Spam={is_spam}")
        
    except Exception as e:
        print(f"Message handling error: {e}")

@app.route('/')
def index():
    """Serve the chat interface"""
    return render_template('index.html')

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return {
        'status': 'healthy',
        'system': 'Priority Chat System',
        'version': '1.0.0',
        'connected_users': len(connected_users)
    }

@app.route('/stats')
def get_stats():
    """Get system statistics"""
    return {
        'connected_users': len(connected_users),
        'access_code_configured': bool(ACCESS_CODE),
        'spam_detection': 'active',
        'uptime': 'running'
    }

def main():
    """Main application entry point"""
    print("=" * 50)
    print("Priority Chat System Starting...")
    print("=" * 50)
    print("Main Interface: http://localhost:5000")
    print("Health Check:   http://localhost:5000/health")
    print("Statistics:     http://localhost:5000/stats")
    print("=" * 50)
    print("Features:")
    print("  ✅ Auto-priority detection")
    print("  ✅ Spam detection and filtering")
    print("  ✅ Real-time messaging")
    print("  ✅ Secret key authentication")
    print("=" * 50)
    print(f"Access Code: {ACCESS_CODE}")
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
        print("\nChat system shutting down...")
    except Exception as e:
        print(f"Fatal error: {str(e)}")

if __name__ == '__main__':
    main()