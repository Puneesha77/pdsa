"""
Priority Chat System - Main Application
A real-time chat system with priority message queues

Author: Your Name
Date: August 2025
"""

from flask import Flask, render_template
from flask_socketio import SocketIO
import logging

# Import our custom modules
from models.priority_queue import PriorityMessageQueue
from handlers.socket_handlers import ChatSocketHandlers


def create_app():
    """
    Application factory pattern - creates and configures the Flask app
    
    Returns:
        Configured Flask app instance
    """
    # Initialize Flask app
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'your-secret-key-change-in-production'
    
    # Configure logging
    if app.debug:
        logging.basicConfig(level=logging.DEBUG)
    
    return app


def create_socketio(app):
    """
    Create and configure SocketIO instance
    
    Args:
        app: Flask app instance
        
    Returns:
        Configured SocketIO instance
    """
    return SocketIO(
        app,
        cors_allowed_origins="*",
        logger=True,
        engineio_logger=True,
        async_mode='threading'
    )


# Create Flask app and SocketIO
app = create_app()
socketio = create_socketio(app)

# Initialize our priority message system
message_system = PriorityMessageQueue(max_history_size=1000)

# Initialize socket handlers
chat_handlers = ChatSocketHandlers(message_system, socketio)


@app.route('/')
def index():
    """
    Main chat page route
    
    Returns:
        Rendered HTML template
    """
    print("📄 Serving Priority Chat interface")
    return render_template('index.html')


@app.route('/health')
def health_check():
    """
    Health check endpoint for monitoring
    
    Returns:
        JSON response with system status
    """
    stats = message_system.get_queue_stats()
    return {
        'status': 'healthy',
        'system': 'Priority Chat System',
        'version': '1.0.0',
        'queue_stats': stats
    }

@app.route('/stats')
def get_stats():
    """
    Get detailed system statistics including batch + retry queues
    """
    base_stats = message_system.get_queue_stats()
    queue_stats = {
        "batch_queue_size": chat_handlers.batch_queue.size(),
        "retry_queue_size": chat_handlers.retry_queue.size(),
        "session_queue_size": chat_handlers.session_queue.size()
    }
    return {
        'message_system': base_stats,
        'queues': queue_stats,
        'connected_users': chat_handlers.get_connected_user_count(),
        'uptime': 'Running'
    }



def main():
    """
    Main application entry point
    """
    print("=" * 50)
    print("🚀 Priority Chat System Starting...")
    print("=" * 50)
    print("📍 Main Interface: http://localhost:5000")
    print("📊 Health Check:  http://localhost:5000/health") 
    print("📈 Statistics:    http://localhost:5000/stats")
    print("=" * 50)
    print("🚨 Priority System Features:")
    print("  ✅ Auto-priority detection (keywords, @mentions, ALL CAPS)")
    print("  ✅ Manual priority override (URGENT/HIGH/NORMAL/LOW)")  
    print("  ✅ Perfect priority ordering display")
    print("  ✅ Real-time message processing")
    print("  ✅ Message validation and sanitization")
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
        print("\n👋 Chat system shutting down...")
    except Exception as e:
        print(f"🚨 Fatal error: {str(e)}")


if __name__ == '__main__':
    main()