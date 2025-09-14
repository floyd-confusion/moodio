"""
User authentication module for the music recommendation system.

Handles user registration, login, password hashing, and session management.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from utils.db import get_db

logger = logging.getLogger(__name__)


class AuthError(Exception):
    """Custom exception for authentication errors"""
    pass


def validate_username(username: str) -> bool:
    """
    Validate username format.
    
    Args:
        username: Username to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not username or not isinstance(username, str):
        return False
    
    username = username.strip()
    if len(username) < 3 or len(username) > 50:
        return False
    
    # Only allow alphanumeric and common safe characters
    allowed_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.')
    if not all(c in allowed_chars for c in username):
        return False
    
    return True


def validate_password(password: str) -> bool:
    """
    Validate password strength.
    
    Args:
        password: Password to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not password or not isinstance(password, str):
        return False
    
    # Minimum length requirement
    if len(password) < 6:
        return False
    
    # Maximum reasonable length
    if len(password) > 128:
        return False
    
    return True


def hash_password(password: str) -> str:
    """
    Hash a password using werkzeug's secure hashing.
    
    Args:
        password: Plain text password
        
    Returns:
        Hashed password string
    """
    return generate_password_hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    """
    Verify a password against its hash.
    
    Args:
        password: Plain text password
        password_hash: Stored password hash
        
    Returns:
        True if password matches, False otherwise
    """
    return check_password_hash(password_hash, password)


def register_user(username: str, password: str) -> Dict[str, Any]:
    """
    Register a new user.
    
    Args:
        username: User's chosen username
        password: User's chosen password
        
    Returns:
        Dictionary with success/error information
        
    Raises:
        AuthError: If registration fails
    """
    # Validate input
    username = username.strip() if username else ""
    
    if not validate_username(username):
        raise AuthError("Invalid username. Must be 3-50 characters, alphanumeric with _-. allowed")
    
    if not validate_password(password):
        raise AuthError("Invalid password. Must be at least 6 characters")
    
    db = get_db()
    
    try:
        # Check if username already exists
        existing_user = db.fetch_one(
            "SELECT id FROM users WHERE username = ?", 
            (username,)
        )
        
        if existing_user:
            raise AuthError("Username already exists")
        
        # Hash password and create user
        password_hash = hash_password(password)
        
        user_id = db.insert('users', {
            'username': username,
            'password_hash': password_hash
        })
        
        logger.info(f"New user registered: {username} (ID: {user_id})")
        
        return {
            'success': True,
            'user_id': user_id,
            'username': username,
            'message': 'User registered successfully'
        }
        
    except AuthError:
        raise
    except Exception as e:
        logger.error(f"Error registering user {username}: {e}")
        raise AuthError("Registration failed")


def authenticate_user(username: str, password: str) -> Dict[str, Any]:
    """
    Authenticate a user login attempt.
    
    Args:
        username: Username
        password: Password
        
    Returns:
        Dictionary with user information if successful
        
    Raises:
        AuthError: If authentication fails
    """
    if not username or not password:
        raise AuthError("Username and password are required")
    
    username = username.strip()
    db = get_db()
    
    try:
        # Get user from database
        user = db.fetch_one(
            "SELECT id, username, password_hash FROM users WHERE username = ?",
            (username,)
        )
        
        if not user:
            raise AuthError("Invalid username or password")
        
        # Verify password
        if not verify_password(password, user['password_hash']):
            logger.warning(f"Failed login attempt for user: {username}")
            raise AuthError("Invalid username or password")
        
        # Update last login time
        db.update('users', 
                 {'last_login': datetime.now().isoformat()}, 
                 'id = ?', 
                 (user['id'],))
        
        logger.info(f"User logged in: {username}")
        
        return {
            'success': True,
            'user_id': user['id'],
            'username': user['username'],
            'message': 'Login successful'
        }
        
    except AuthError:
        raise
    except Exception as e:
        logger.error(f"Error authenticating user {username}: {e}")
        raise AuthError("Authentication failed")


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Get user information by ID.
    
    Args:
        user_id: User ID
        
    Returns:
        User dictionary or None if not found
    """
    try:
        db = get_db()
        user = db.fetch_one(
            "SELECT id, username, created_at, last_login, playback_type FROM users WHERE id = ?",
            (user_id,)
        )

        if user:
            return {
                'id': user['id'],
                'username': user['username'],
                'created_at': user['created_at'],
                'last_login': user['last_login'],
                'playback_type': user['playback_type'] or 'spotify'  # Default to spotify if null
            }
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting user by ID {user_id}: {e}")
        return None


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    """
    Get user information by username.
    
    Args:
        username: Username
        
    Returns:
        User dictionary or None if not found
    """
    try:
        username = username.strip() if username else ""
        db = get_db()
        user = db.fetch_one(
            "SELECT id, username, created_at, last_login FROM users WHERE username = ?",
            (username,)
        )
        
        if user:
            return {
                'id': user['id'],
                'username': user['username'],
                'created_at': user['created_at'],
                'last_login': user['last_login']
            }
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting user by username {username}: {e}")
        return None


def delete_user(user_id: int) -> bool:
    """
    Delete a user and all associated data.
    
    Args:
        user_id: User ID to delete
        
    Returns:
        True if user was deleted successfully
    """
    try:
        db = get_db()
        
        with db.transaction():
            # Delete user (CASCADE will handle sessions and related data)
            rows_affected = db.delete('users', 'id = ?', (user_id,))
            
        if rows_affected > 0:
            logger.info(f"User {user_id} deleted successfully")
            return True
        else:
            logger.warning(f"User {user_id} not found for deletion")
            return False
            
    except Exception as e:
        logger.error(f"Error deleting user {user_id}: {e}")
        return False