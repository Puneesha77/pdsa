"""
Priority Chat System - Combined Application
A real-time chat system with Priority Queue, Batch Queue, and Retry Queue

Author: Team Project
Date: August 2025
"""

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from flask_cors import CORS
import logging
import sys
import os

# Import message systems
from models.priority_queue import PriorityMessageQueue
from models.enhanced_message_system import EnhancedMessageSystem
from handlers.socket_handlers import ChatSocketHandlers


def create_app():
    """Application factory pattern"""
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'your-secret-key-change-in-production'
    
    # Enable CORS for all routes
    CORS(app, resources={r"/*": {"origins": "*"}})
    
    # Configure logging
    if app.debug:
        logging.basicConfig(level=logging.DEBUG)
        app.logger.setLevel(logging.DEBUG)
    
    return app


def create_socketio(app):
    """Create and configure SocketIO instance"""
    return SocketIO(
        app,
        cors_allowed_origins="*",
        logger=True,
        engineio_logger=True,
        async_mode='threading',
        ping_timeout=60,
        ping_interval=25
    )


# Create Flask app and SocketIO
app = create_app()
socketio = create_socketio(app)

# Initialize message systems with error handling
try:
    basic_message_system = PriorityMessageQueue(max_history_size=1000)
    print("✅ Basic Priority Queue initialized")
    
    enhanced_message_system = EnhancedMessageSystem(
        max_history_size=1000,
        min_batch_size=5,
        max_batch_size=50,
        batch_timeout=2.0,
        max_retries=5
    )
    print("✅ Enhanced Message System initialized")
    
    # Initialize handlers
    chat_handlers = ChatSocketHandlers(enhanced_message_system.priority_queue, socketio)
    print("✅ Socket handlers initialized")
    
except Exception as e:
    print(f"🚨 Error initializing systems: {e}")
    sys.exit(1)


@app.route('/')
def index():
    """Main chat page route"""
    print("📄 Serving Combined Priority Chat interface")
    try:
        return render_template('index.html')
    except Exception as e:
        print(f"🚨 Error serving index.html: {e}")
        return f"<h1>Error: {e}</h1><p>Check if templates/index.html exists</p>", 500

# --------------------
# 🔧 ADDED: Dependency checking function
# --------------------

def check_dependencies():
    """Check if required files exist"""
    required_files = [
        'templates/index.html',
        'models/priority_queue.py',
        'models/enhanced_message_system.py',
        'handlers/socket_handlers.py'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("🚨 Missing required files:")
        for file in missing_files:
            print(f"   ❌ {file}")
        return False
    
    print("✅ All required files found")
    return True


# --------------------
# 🔧 ADDED: Error Handlers for better debugging
# --------------------

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


# --------------------
# 🌐 Monitoring Routes (with error handling)
# --------------------

@app.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        stats = enhanced_message_system.get_comprehensive_stats()
        return jsonify({
            'status': 'healthy',
            'system': 'Combined Priority Chat System',
            'version': '2.0.0',
            'components': ['Priority Queue', 'Batch Queue', 'Retry Queue'],
            'stats': stats.get('system_overview', {})
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/stats')
def get_stats():
    """Get combined system statistics"""
    try:
        return jsonify({
            'basic_priority_queue': basic_message_system.get_queue_stats(),
            'enhanced_message_system': enhanced_message_system.get_comprehensive_stats(),
            'queue_status': enhanced_message_system.get_queue_status(),
            'connected_users': chat_handlers.get_connected_user_count() if chat_handlers else 0
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/batch_stats')
def batch_stats():
    """Batch queue statistics endpoint"""
    try:
        stats = enhanced_message_system.batch_queue.get_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/retry_stats')
def retry_stats():
    """Retry queue statistics endpoint"""
    try:
        stats = enhanced_message_system.retry_queue.get_queue_contents()
        return jsonify({
            'queue_size': len(stats),
            'failed_messages': stats,
            'total_retries': sum(msg.get('retry_count', 0) for msg in stats)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# --------------------
# 🛠️ Admin Routes (with error handling)
# --------------------

@app.route('/admin/batch/force-send', methods=['GET', 'POST'])
def force_send_batches():
    """Force send pending batches"""
    try:
        count = enhanced_message_system.force_send_pending_batches()
        return jsonify({'action': 'force_send_batches', 'messages_sent': count, 'success': True})
    except Exception as e:
        return jsonify({'action': 'force_send_batches', 'error': str(e), 'success': False}), 500


@app.route('/admin/retry/force-all', methods=['GET', 'POST'])
def force_retry_all():
    """Force retry failed messages"""
    try:
        count = enhanced_message_system.retry_failed_messages()
        return jsonify({'action': 'force_retry_all', 'messages_marked_for_retry': count, 'success': True})
    except Exception as e:
        return jsonify({'action': 'force_retry_all', 'error': str(e), 'success': False}), 500


@app.route('/admin/clear-all', methods=['GET', 'POST'])
def clear_all():
    """Clear all queues"""
    try:
        cleared_counts = enhanced_message_system.clear_all_queues()
        return jsonify({'action': 'clear_all_queues', 'cleared': cleared_counts, 'success': True})
    except Exception as e:
        return jsonify({'action': 'clear_all_queues', 'error': str(e), 'success': False}), 500


@app.route('/admin/detailed-stats')
def detailed_stats():
    """Detailed system stats for debugging"""
    try:
        return jsonify({
            'comprehensive_stats': enhanced_message_system.get_comprehensive_stats(),
            'queue_contents': {
                'batch_queue': enhanced_message_system.batch_queue.get_stats(),
                'retry_queue': enhanced_message_system.retry_queue.get_queue_contents()
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# --------------------
# 🔧 Error Handlers
# --------------------

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


# --------------------
# 🚀 Entry Point
# --------------------

def check_dependencies():
    """Check if required files exist"""
    required_files = [
        'templates/index.html',
        'models/priority_queue.py',
        'models/enhanced_message_system.py',
        'handlers/socket_handlers.py'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("🚨 Missing required files:")
        for file in missing_files:
            print(f"   ❌ {file}")
        return False
    
    print("✅ All required files found")
    return True


def main():
    """Main entry point"""
    print("=" * 70)
    print("🚀 Combined Priority Chat System Starting...")
    print("=" * 70)
    
    # Check dependencies first
    if not check_dependencies():
        print("🚨 Cannot start server - missing required files")
        sys.exit(1)
    
    print("📍 Main Interface:    http://localhost:5000")
    print("📊 Health Check:      http://localhost:5000/health") 
    print("📈 Statistics:        http://localhost:5000/stats")
    print("📊 Batch Stats:       http://localhost:5000/batch_stats")
    print("📊 Retry Stats:       http://localhost:5000/retry_stats")
    print("📊 Detailed Stats:    http://localhost:5000/admin/detailed-stats")
    print("🔧 Force Batch Send:  http://localhost:5000/admin/batch/force-send")
    print("🔄 Force Retry All:   http://localhost:5000/admin/retry/force-all")
    print("🧹 Clear All Queues:  http://localhost:5000/admin/clear-all")
    print("=" * 70)
    print("🏗️ System Architecture:")
    print("  ✅ Priority Queue (Min-Heap) - message prioritization")
    print("  ✅ Circular Queue - history (1000 messages)")
    print("  ✅ Batch Queue (FIFO) - groups 5-50 msgs, 2s timeout")
    print("  ✅ Retry Queue (Deque) - failed message handling, 5 retries")
    print("=" * 70)
    print("🌐 Starting server...")

    try:
        socketio.run(
            app,
            debug=True,
            port=5000,
            host='0.0.0.0',  # Allow external connections
            allow_unsafe_werkzeug=True,
            use_reloader=False  # Prevent double initialization
        )
    except KeyboardInterrupt:
        print("\n👋 Shutting down Chat System...")
        try:
            enhanced_message_system.shutdown()
        except:
            pass
    except Exception as e:
        print(f"🚨 Fatal error: {str(e)}")
        try:
            enhanced_message_system.shutdown()
        except:
            pass
        sys.exit(1)


if __name__ == '__main__':
    main()