"""
Message Utility Functions
Helper functions for message processing and validation
"""

import re
import html
import time
from typing import Dict, List, Optional
from datetime import datetime


def validate_message(message: str) -> Dict:
    """
    Validate and sanitize a message
    
    Args:
        message: Raw message text
        
    Returns:
        Dict with validation result and sanitized message
    """
    # Remove extra whitespace
    message = message.strip()
    
    # Check if empty
    if not message:
        return {
            'valid': False,
            'error': 'Message cannot be empty',
            'sanitized_message': ''
        }
    
    # Check length limits
    if len(message) > 2000:
        return {
            'valid': False,
            'error': 'Message too long (max 2000 characters)',
            'sanitized_message': message[:2000]
        }
    
    # Sanitize HTML to prevent XSS attacks
    sanitized = html.escape(message)
    
    # Basic profanity filter (you can extend this)
    profanity_words = ['badword1', 'badword2']  # Add actual words if needed
    contains_profanity = any(word in sanitized.lower() for word in profanity_words)
    
    return {
        'valid': True,
        'sanitized_message': sanitized,
        'contains_profanity': contains_profanity,
        'original_length': len(message),
        'sanitized_length': len(sanitized)
    }


def validate_username(username: str) -> Dict:
    """
    Validate and sanitize a username
    
    Args:
        username: Raw username
        
    Returns:
        Dict with validation result and sanitized username
    """
    # Remove extra whitespace
    username = username.strip()
    
    # Default to Anonymous if empty
    if not username:
        username = "Anonymous"
    
    # Check length
    if len(username) > 50:
        username = username[:50]
    
    # Remove special characters except underscore and dash
    username = re.sub(r'[^\w\-\s]', '', username)
    
    # Sanitize HTML
    username = html.escape(username)
    
    return {
        'valid': True,
        'sanitized_username': username,
        'was_modified': username != username
    }


def format_message_for_client(message_obj: Dict) -> Dict:
    """
    Format a message object for sending to client
    
    Args:
        message_obj: Message object from priority queue
        
    Returns:
        Formatted message dict for client
    """
    return {
        'text': message_obj['text'],
        'user': message_obj['user'],
        'priority': message_obj['priority'],
        'priority_name': message_obj['priority_name'],
        'timestamp': message_obj['timestamp'],
        'formatted_time': format_timestamp(message_obj['timestamp']),
        'detection_method': message_obj.get('detection_method', 'unknown')
    }


def format_timestamp(timestamp: float) -> str:
    """
    Format a timestamp for display
    
    Args:
        timestamp: Unix timestamp
        
    Returns:
        Formatted time string
    """
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime("%H:%M:%S")


def detect_message_type(message: str) -> Dict:
    """
    Detect what type of message this is
    
    Args:
        message: Message text
        
    Returns:
        Dict with message type information
    """
    message_lower = message.lower()
    
    # Check for different message types
    is_question = message.strip().endswith('?')
    is_command = message.startswith('/')
    is_mention = '@' in message
    is_url = 'http' in message_lower or 'www.' in message_lower
    is_caps = len(message) > 5 and message.isupper()
    has_emoji = bool(re.search(r'[😀-🿿]', message))  # Basic emoji detection
    
    return {
        'is_question': is_question,
        'is_command': is_command,
        'is_mention': is_mention,
        'is_url': is_url,
        'is_caps': is_caps,
        'has_emoji': has_emoji,
        'word_count': len(message.split()),
        'char_count': len(message)
    }


def get_priority_stats(messages: List[Dict]) -> Dict:
    """
    Get statistics about priority distribution in messages
    
    Args:
        messages: List of message objects
        
    Returns:
        Dict with priority statistics
    """
    if not messages:
        return {'total': 0, 'breakdown': {}}
    
    breakdown = {}
    for msg in messages:
        priority_name = msg['priority_name']
        breakdown[priority_name] = breakdown.get(priority_name, 0) + 1
    
    return {
        'total': len(messages),
        'breakdown': breakdown,
        'most_common_priority': max(breakdown, key=breakdown.get) if breakdown else None
    }


def create_system_message(text: str, priority: int = 3) -> Dict:
    """
    Create a system message (from the server)
    
    Args:
        text: System message text
        priority: Message priority (default: NORMAL)
        
    Returns:
        System message object
    """
    return {
        'text': text,
        'user': 'SYSTEM',
        'priority': priority,
        'priority_name': {1: "URGENT", 2: "HIGH", 3: "NORMAL", 4: "LOW"}[priority],
        'timestamp': time.time(),
        'detection_method': 'system',
        'is_system': True
    }