from flask import Flask, send_from_directory, jsonify, request, session
import logging
from datetime import datetime
from src.dataset import genre_groups
from src.session import Session, get_all_sessions, delete_session
from src.auth import register_user, authenticate_user, get_user_by_id, AuthError
from src.filters import FILTER_REGISTRY

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='public')
app.secret_key = 'music-recommendation-system-key'  # Enable sessions

def get_current_session(session_id=None):
    """Get or create the current session for the current user (no global state)"""

    # If a specific session_id is requested, load that session
    if session_id is not None:
        try:
            requested_session = Session(session_id)
            # Verify user access if they are logged in
            user_id = session.get('user_id')
            if user_id and requested_session.user_id and requested_session.user_id != user_id:
                logger.warning(f"User {user_id} denied access to session {session_id} (owner: {requested_session.user_id})")
                return None
            logger.info(f"Loaded specific session {session_id}: {requested_session.name}")
            return requested_session
        except Exception as e:
            logger.error(f"Failed to load session {session_id}: {e}")
            return None

    # Get active session for current user from Flask session storage
    stored_session_id = session.get('active_session_id')
    if stored_session_id:
        try:
            user_session = Session(stored_session_id)
            # Verify ownership
            user_id = session.get('user_id')
            if user_id and user_session.user_id and user_session.user_id != user_id:
                logger.warning(f"Session ownership mismatch for user {user_id}, clearing session")
                session.pop('active_session_id', None)
            else:
                logger.debug(f"Using active session {stored_session_id}: {user_session.name}")
                return user_session
        except Exception as e:
            logger.warning(f"Failed to load active session {stored_session_id}: {e}")
            session.pop('active_session_id', None)

    # No active session found, create or find one for this user
    user_id = session.get('user_id')

    if user_id:
        # For logged-in users, try to get their most recent session
        user_sessions = get_all_sessions(user_id)
        if user_sessions:
            session_info = user_sessions[0]
            user_session = Session(session_info['id'], session_info['name'])
            session['active_session_id'] = user_session.session_id
            logger.info(f"Auto-selected user's most recent session: {session_info['name']}")
            return user_session
        else:
            # Create a default session for the user (shouldn't happen with new flow, but fallback)
            user_session = Session.create_new("My First Session", user_id)
            session['active_session_id'] = user_session.session_id
            logger.info(f"Created fallback session for user {user_id}")
            return user_session
    else:
        # For anonymous users, create a temporary session (shouldn't happen in new flow)
        anonymous_session = Session.create_new("Guest Session")
        logger.info("Created temporary anonymous session")
        return anonymous_session

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'auth.html')

@app.route('/genres')
def serve_genres():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/auth')
def serve_auth():
    return send_from_directory(app.static_folder, 'auth.html')

@app.route('/sessions')
def serve_sessions():
    return send_from_directory(app.static_folder, 'sessions.html')

@app.route('/api/genres')
def get_genres():
    logger.info("API: Getting available genres")
    genres =  list(genre_groups.keys())
    return jsonify(genres)

# @app.route('/api/set_genre', methods=['POST'])
# def set_genre():
#     """Set the genre pool based on the selected genre group"""
#     data = request.get_json()
#     if not data or 'genre' not in data:
#         logger.warning("API: set_genre called without genre data")
#         return jsonify({'error': 'No genre provided'}), 400
#
#     genre_group = data['genre']
#     logger.info(f"API: Setting genre pool to {genre_group}")
#     session_obj = get_current_session()
#     dataset = session_obj.get_dataset()
#
#     if dataset.set_genre_pool(genre_group):
#         # Update session metadata with selected genre
#         session_obj.update_session_metadata(genre_group=genre_group)
#         session_obj.save_state()
#         return jsonify({
#             'message': f'Genre pool set for {genre_group}',
#             'genre_pool_size': len(dataset.genre_pool),
#             'playback_pool_size': len(dataset.playback_pool),
#             'filters_cleared': True
#         })
#     return jsonify({'error': 'Invalid genre group or no tracks found'}), 400


# @app.route('/api/adjust_pool', methods=['POST'])
# def adjust_pool():
#     adjustment = request.json.get('adjustment')
#     if adjustment is None:
#         logger.warning("API: adjust_pool called without adjustment value")
#         return jsonify({'error': 'No adjustment value provided'}), 400
#
#     try:
#         adjustment = int(adjustment)
#         if not 0 <= adjustment <= 15:
#             logger.warning(f"API: Invalid adjustment value {adjustment}")
#             return jsonify({'error': 'Adjustment must be between 0 and 15'}), 400
#     except ValueError:
#         logger.warning(f"API: Non-integer adjustment value: {adjustment}")
#         return jsonify({'error': 'Invalid adjustment value'}), 400
#
#     logger.info(f"API: Processing pool adjustment {adjustment}")
#     session_obj = get_current_session()
#     dataset = session_obj.get_dataset()
#
#     result, status_code = dataset.adjust_pool(adjustment)
#     return jsonify(result), status_code


@app.route('/api/track/<track_id>')
def get_track_by_id(track_id):
    """Get track details by ID"""
    logger.debug(f"API: Getting track by ID: {track_id}")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    track = dataset.get_track_by_id(track_id)
    if track is None:
        logger.warning(f"API: Track not found: {track_id}")
        return jsonify({'error': 'Track not found'}), 404
    return jsonify(track)

# @app.route('/api/pool_stats')
# def get_pool_stats():
#     """Get current pool statistics"""
#     logger.debug("API: Getting pool statistics")
#     session_obj = get_current_session()
#     dataset = session_obj.get_dataset()
#
#     stats = dataset.get_pool_stats()
#     return jsonify(stats)

# @app.route('/api/adjustment_history')
# def get_adjustment_history():
#     """Get adjustment history"""
#     logger.debug("API: Getting adjustment history")
#     session_obj = get_current_session()
#     dataset = session_obj.get_dataset()
#
#     history = dataset.get_adjustment_history()
#     return jsonify({'adjustments': history})

# Authentication Endpoints
@app.route('/api/register', methods=['POST'])
def register():
    """Register a new user"""
    data = request.get_json()
    if not data:
        logger.warning("API: register called without data")
        return jsonify({'error': 'Registration data required'}), 400
    
    username = data.get('username', '').strip()
    password = data.get('password', '')
    
    if not username or not password:
        logger.warning("API: register called without username or password")
        return jsonify({'error': 'Username and password are required'}), 400
    
    try:
        result = register_user(username, password)
        logger.info(f"API: User registered successfully: {username}")
        return jsonify(result), 201
        
    except AuthError as e:
        logger.warning(f"API: Registration failed for {username}: {str(e)}")
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"API: Registration error for {username}: {e}")
        return jsonify({'error': 'Registration failed'}), 500

@app.route('/api/login', methods=['POST'])
def login():
    """Authenticate user login"""
    data = request.get_json()
    if not data:
        logger.warning("API: login called without data")
        return jsonify({'error': 'Login data required'}), 400
    
    username = data.get('username', '').strip()
    password = data.get('password', '')
    
    if not username or not password:
        logger.warning("API: login called without username or password")
        return jsonify({'error': 'Username and password are required'}), 400
    
    try:
        result = authenticate_user(username, password)
        
        # Store user info in Flask session
        session['user_id'] = result['user_id']
        session['username'] = result['username']
        
        logger.info(f"API: User logged in successfully: {username}")
        return jsonify(result)
        
    except AuthError as e:
        logger.warning(f"API: Login failed for {username}: {str(e)}")
        return jsonify({'error': str(e)}), 401
    except Exception as e:
        logger.error(f"API: Login error for {username}: {e}")
        return jsonify({'error': 'Login failed'}), 500

@app.route('/api/logout', methods=['POST'])
def logout():
    """Logout user and clear session"""
    username = session.get('username', 'unknown')
    
    # Clear Flask session
    session.clear()
    
    logger.info(f"API: User logged out: {username}")
    return jsonify({'message': 'Logged out successfully'})

@app.route('/api/user', methods=['GET'])
def get_current_user():
    """Get current user information"""
    user_id = session.get('user_id')
    
    if not user_id:
        return jsonify({'user': None, 'authenticated': False})
    
    user_info = get_user_by_id(user_id)
    
    if user_info:
        return jsonify({
            'user': user_info,
            'authenticated': True
        })
    else:
        # Clear invalid session
        session.clear()
        return jsonify({'user': None, 'authenticated': False})

@app.route('/api/sessions/<int:session_id>', methods=['PUT'])
def update_session(session_id):
    """Update session name"""
    data = request.get_json()
    if not data or 'name' not in data:
        logger.warning(f"API: update_session called for {session_id} without name")
        return jsonify({'error': 'Session name is required'}), 400

    name = data['name'].strip()
    if not name:
        logger.warning(f"API: update_session called for {session_id} with empty name")
        return jsonify({'error': 'Session name cannot be empty'}), 400

    try:
        from utils.db import get_db
        db = get_db()
        rows_affected = db.update('sessions', {'name': name}, 'id = ?', (session_id,))

        if rows_affected > 0:
            logger.info(f"API: Updated session {session_id} name to: {name}")
            return jsonify({'message': f'Session name updated to "{name}"'})
        else:
            logger.warning(f"API: Session {session_id} not found for update")
            return jsonify({'error': 'Session not found'}), 404

    except Exception as e:
        logger.error(f"API: Error updating session {session_id}: {e}")
        return jsonify({'error': 'Failed to update session'}), 500

@app.route('/api/sessions/<int:session_id>', methods=['DELETE'])
def delete_session_endpoint(session_id):
    """Delete a session"""
    logger.info(f"API: Deleting session {session_id}")

    try:
        # Check if user has access to delete this session
        session_obj = Session(session_id)
        user_id = session.get('user_id')

        if session_obj.user_id is not None and session_obj.user_id != user_id:
            logger.warning(f"API: User {user_id} denied delete access to session {session_id} (owner: {session_obj.user_id})")
            return jsonify({'error': 'Access denied to delete this session'}), 403

        # Don't allow deleting the current active session
        active_session_id = session.get('active_session_id')
        if active_session_id == session_id:
            logger.warning(f"API: Cannot delete active session {session_id}")
            return jsonify({'error': 'Cannot delete the currently active session'}), 400

        if delete_session(session_id):
            logger.info(f"API: Session {session_id} deleted successfully")
            return jsonify({'message': 'Session deleted successfully'})
        else:
            logger.warning(f"API: Session {session_id} not found for deletion")
            return jsonify({'error': 'Session not found'}), 404

    except Exception as e:
        logger.warning(f"API: Session {session_id} not found: {e}")
        return jsonify({'error': 'Session not found'}), 404

# @app.route('/api/current_session', methods=['GET'])
# def get_current_session_info():
#     """Get current session information"""
#     logger.debug("API: Getting current session info")
#     session_obj = get_current_session()
#     return jsonify({'current_session': session_obj.get_session_info()})
#
# @app.route('/api/current_session', methods=['POST'])
# def set_current_session():
#     """Set the current active session"""
#     logger.debug("API: Setting current session")
#
#     data = request.get_json()
#     if not data or 'session_id' not in data:
#         logger.warning("API: set_current_session called without session_id")
#         return jsonify({'error': 'Session ID is required'}), 400
#
#     session_id = data['session_id']
#     try:
#         session_id = int(session_id)
#     except ValueError:
#         logger.warning(f"API: Invalid session_id: {session_id}")
#         return jsonify({'error': 'Invalid session ID'}), 400
#
#     # Switch to the requested session
#     session_obj = get_current_session(session_id)
#     if session_obj is None:
#         logger.warning(f"API: Could not access session {session_id}")
#         return jsonify({'error': 'Session not found or access denied'}), 404
#
#     # Store the active session ID in Flask session
#     session['active_session_id'] = session_id
#
#     logger.info(f"API: Current session set to {session_id}")
#     return jsonify({
#         'message': 'Current session updated',
#         'current_session': session_obj.get_session_info()
#     })

@app.route('/api/user/<int:user_id>/sessions', methods=['GET'])
def get_user_sessions(user_id):
    """Get all sessions for a specific user"""
    logger.info(f"API: Getting sessions for user {user_id}")

    # Check if user is authenticated and matches the requested user_id
    current_user_id = session.get('user_id')
    if not current_user_id:
        logger.warning("API: Unauthenticated user trying to access sessions")
        return jsonify({'error': 'Authentication required'}), 401

    if current_user_id != user_id:
        logger.warning(f"API: User {current_user_id} trying to access sessions for user {user_id}")
        return jsonify({'error': 'Access denied'}), 403

    try:
        from src.session import get_all_sessions
        sessions = get_all_sessions(user_id)

        # Enhance session data with metadata
        enhanced_sessions = []
        for session_info in sessions:
            try:
                session_obj = Session(session_info['id'])
                enhanced_info = session_obj.get_session_info()
                enhanced_sessions.append(enhanced_info)
            except Exception as e:
                logger.warning(f"Error enhancing session {session_info['id']}: {e}")
                enhanced_sessions.append(session_info)

        logger.info(f"API: Returning {len(enhanced_sessions)} sessions for user {user_id}")
        return jsonify({'sessions': enhanced_sessions})

    except Exception as e:
        logger.error(f"API: Error getting sessions for user {user_id}: {e}")
        return jsonify({'error': 'Failed to retrieve sessions'}), 500

@app.route('/api/user/<int:user_id>/sessions', methods=['POST'])
def create_user_session(user_id):
    """Create a new session for a specific user"""
    logger.info(f"API: Creating new session for user {user_id}")
    
    # Check if user is authenticated and matches the requested user_id
    current_user_id = session.get('user_id')
    if not current_user_id:
        logger.warning("API: Unauthenticated user trying to create session")
        return jsonify({'error': 'Authentication required'}), 401
    
    if current_user_id != user_id:
        logger.warning(f"API: User {current_user_id} trying to create session for user {user_id}")
        return jsonify({'error': 'Access denied'}), 403
    
    try:
        data = request.get_json()
        if not data or 'name' not in data or 'genre' not in data:
            logger.warning("API: create_user_session called without required parameters")
            return jsonify({'error': 'Session name and genre are required'}), 400

        session_name = data['name'].strip()
        genre_group = data['genre'].strip()

        if not session_name:
            logger.warning("API: create_user_session called with empty session name")
            return jsonify({'error': 'Session name cannot be empty'}), 400

        if not genre_group:
            logger.warning("API: create_user_session called with empty genre")
            return jsonify({'error': 'Genre is required'}), 400

        # Validate genre group
        from src.dataset import genre_groups
        if genre_group not in genre_groups:
            logger.warning(f"API: Invalid genre group: {genre_group}")
            return jsonify({'error': 'Invalid genre group'}), 400

        # Create new session
        new_session = Session.create_new(session_name, user_id)

        # Set the genre pool immediately
        dataset = new_session.get_dataset()
        if dataset.set_genre_pool(genre_group):
            # Update session metadata with genre
            new_session.update_session_metadata(genre_group=genre_group)
            new_session.save_state()
        else:
            # Clean up the session if genre setting failed
            delete_session(new_session.session_id)
            return jsonify({'error': f'Failed to set genre pool for {genre_group}'}), 400

        session_info = new_session.get_session_info()

        logger.info(f"API: Created new session {new_session.session_id} for user {user_id}: {session_name} ({genre_group})")
        return jsonify({
            'message': 'Session created successfully',
            'session': session_info
        }), 201
        
    except Exception as e:
        logger.error(f"API: Error creating session for user {user_id}: {e}")
        return jsonify({'error': 'Failed to create session'}), 500

@app.route('/api/user/<int:user_id>/sessions/current', methods=['GET'])
def get_user_current_session(user_id):
    """Get the current active session for a user"""
    logger.debug(f"API: Getting current session for user {user_id}")

    # Check if user is authenticated and matches the requested user_id
    current_user_id = session.get('user_id')
    if not current_user_id:
        logger.warning("API: Unauthenticated user trying to get current session")
        return jsonify({'error': 'Authentication required'}), 401

    if current_user_id != user_id:
        logger.warning(f"API: User {current_user_id} trying to get current session for user {user_id}")
        return jsonify({'error': 'Access denied'}), 403

    try:
        # Get current session ID from Flask session
        current_session_id = session.get('current_session_id')

        if not current_session_id:
            logger.info(f"API: No current session set for user {user_id}")
            return jsonify({'current_session': None})

        # Verify the session exists and belongs to the user
        from src.session import Session
        try:
            session_obj = Session(current_session_id)
            if session_obj.user_id != user_id:
                logger.warning(f"API: Session {current_session_id} does not belong to user {user_id}")
                # Clear invalid session
                session.pop('current_session_id', None)
                return jsonify({'current_session': None})
        except Exception:
            logger.warning(f"API: Session {current_session_id} not found, clearing from session")
            session.pop('current_session_id', None)
            return jsonify({'current_session': None})

        # Return current session info
        session_info = session_obj.get_session_info()
        return jsonify({'current_session': session_info})

    except Exception as e:
        logger.error(f"API: Error getting current session for user {user_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/user/<int:user_id>/sessions/current', methods=['PUT'])
def set_user_current_session(user_id):
    """Set the current active session for a user"""
    logger.debug(f"API: Setting current session for user {user_id}")

    # Check if user is authenticated and matches the requested user_id
    current_user_id = session.get('user_id')
    if not current_user_id:
        logger.warning("API: Unauthenticated user trying to set current session")
        return jsonify({'error': 'Authentication required'}), 401

    if current_user_id != user_id:
        logger.warning(f"API: User {current_user_id} trying to set current session for user {user_id}")
        return jsonify({'error': 'Access denied'}), 403

    try:
        data = request.get_json()
        if not data or 'session_id' not in data:
            logger.warning("API: set_current_session called without session_id")
            return jsonify({'error': 'session_id is required'}), 400

        session_id = data['session_id']

        # Verify the session exists and belongs to the user
        from src.session import Session
        try:
            session_obj = Session(session_id)
            if session_obj.user_id != user_id:
                logger.warning(f"API: User {user_id} trying to set session {session_id} that belongs to user {session_obj.user_id}")
                return jsonify({'error': 'Session access denied'}), 403
        except Exception as e:
            logger.warning(f"API: Session {session_id} not found: {e}")
            return jsonify({'error': 'Session not found'}), 404

        # Set the current session in Flask session
        session['current_session_id'] = session_id

        logger.info(f"API: Set current session {session_id} for user {user_id}")
        return jsonify({
            'message': 'Current session set successfully',
            'current_session_id': session_id
        })

    except Exception as e:
        logger.error(f"API: Error setting current session for user {user_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/user/<int:user_id>/sessions/<int:session_id>/filters', methods=['POST'])
def add_session_filter(user_id, session_id):
    """Add a named filter to a specific session"""
    logger.info(f"API: Adding named filter to session {session_id} for user {user_id}")

    # Check if user is authenticated and matches the requested user_id
    current_user_id = session.get('user_id')
    if not current_user_id:
        logger.warning("API: Unauthenticated user trying to add filter to session")
        return jsonify({'error': 'Authentication required'}), 401

    if current_user_id != user_id:
        logger.warning(f"API: User {current_user_id} trying to add filter to session for user {user_id}")
        return jsonify({'error': 'Access denied'}), 403

    try:
        # Load the session and verify ownership
        session_obj = Session(session_id)
        if session_obj.user_id != user_id:
            logger.warning(f"API: User {user_id} trying to add filter to session {session_id} owned by {session_obj.user_id}")
            return jsonify({'error': 'Access denied to this session'}), 403

        data = request.get_json()
        try:
            filter_type = data['filter_type']
            assert filter_type in FILTER_REGISTRY, f"Unknown filter type: {filter_type}"
        except:
            logger.warning("API: add_session_filter called without filter_type or invalid filter_type value")
            return jsonify({'error': 'filter_type is required'}), 400

        logger.info(f"API: Adding filter {filter_type} to session {session_id}")

        # Add filter to session
        if not session_obj.add_filter(filter_type):
            logger.error(f"API: Failed to add filter {filter_type} to session {session_id}")
            return jsonify({'error': 'Failed to add filter to session'}), 500

        return jsonify({
            'message': 'Filter added successfully',
            'filter_applied': filter_type,
            'session_id': session_id
        })

    except Exception as e:
        logger.error(f"API: Error adding filter to session {session_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/user/<int:user_id>/sessions/<int:session_id>/likes', methods=['GET'])
def get_session_likes(user_id, session_id):
    """Get all liked tracks for a specific session"""
    logger.debug(f"API: Getting liked tracks for session {session_id} for user {user_id}")

    # Check if user is authenticated and matches the requested user_id
    current_user_id = session.get('user_id')
    if not current_user_id:
        logger.warning("API: Unauthenticated user trying to get session likes")
        return jsonify({'error': 'Authentication required'}), 401

    if current_user_id != user_id:
        logger.warning(f"API: User {current_user_id} trying to get session likes for user {user_id}")
        return jsonify({'error': 'Access denied'}), 403

    try:
        # Load the session and verify ownership
        from src.session import Session
        try:
            session_obj = Session(session_id)
        except Exception as e:
            logger.warning(f"API: Session {session_id} not found: {e}")
            return jsonify({'error': 'Session not found'}), 404

        # Verify session ownership
        if session_obj.user_id != user_id:
            logger.warning(f"API: User {user_id} trying to get likes from session {session_id} owned by {session_obj.user_id}")
            return jsonify({'error': 'Session access denied'}), 403

        # Get liked track IDs
        liked_track_ids = session_obj.get_liked_track_ids()

        logger.info(f"API: Returning {len(liked_track_ids)} liked tracks for session {session_id}")
        return jsonify({
            'liked_tracks': liked_track_ids,
            'count': len(liked_track_ids)
        })

    except Exception as e:
        logger.error(f"API: Error getting likes for session {session_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/user/<int:user_id>/sessions/<int:session_id>/likes', methods=['POST'])
def add_session_like(user_id, session_id):
    """Add a track to liked tracks for a specific session"""
    logger.debug(f"API: Adding like to session {session_id} for user {user_id}")

    # Check if user is authenticated and matches the requested user_id
    current_user_id = session.get('user_id')
    if not current_user_id:
        logger.warning("API: Unauthenticated user trying to add session like")
        return jsonify({'error': 'Authentication required'}), 401

    if current_user_id != user_id:
        logger.warning(f"API: User {current_user_id} trying to add session like for user {user_id}")
        return jsonify({'error': 'Access denied'}), 403

    try:
        # Load the session and verify ownership
        from src.session import Session
        try:
            session_obj = Session(session_id)
        except Exception as e:
            logger.warning(f"API: Session {session_id} not found: {e}")
            return jsonify({'error': 'Session not found'}), 404

        # Verify session ownership
        if session_obj.user_id != user_id:
            logger.warning(f"API: User {user_id} trying to add like to session {session_id} owned by {session_obj.user_id}")
            return jsonify({'error': 'Session access denied'}), 403

        data = request.get_json()
        if not data or 'track_id' not in data:
            logger.warning("API: add_session_like called without track_id")
            return jsonify({'error': 'track_id is required'}), 400

        track_id = data['track_id']
        if not track_id or not isinstance(track_id, str):
            logger.warning(f"API: Invalid track_id: {track_id}")
            return jsonify({'error': 'Valid track_id is required'}), 400

        # Add the track to likes
        was_added = session_obj.add_liked_track_by_id(track_id)

        if was_added:
            logger.info(f"API: Added track {track_id} to likes for session {session_id}")
            return jsonify({
                'message': 'Track added to likes',
                'track_id': track_id,
                'was_new': True
            })
        else:
            logger.info(f"API: Track {track_id} already liked in session {session_id}")
            return jsonify({
                'message': 'Track already liked',
                'track_id': track_id,
                'was_new': False
            })

    except Exception as e:
        logger.error(f"API: Error adding like to session {session_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/user/<int:user_id>/sessions/<int:session_id>/track', methods=['GET'])
def get_user_session_track(user_id, session_id):
    """Get a random track from the specified user session"""
    logger.debug(f"API: Getting track for user {user_id} from session {session_id}")

    # Check if user is authenticated and matches the requested user_id
    current_user_id = session.get('user_id')
    if not current_user_id:
        logger.warning("API: Unauthenticated user trying to get track")
        return jsonify({'error': 'Authentication required'}), 401

    if current_user_id != user_id:
        logger.warning(f"API: User {current_user_id} trying to get track for user {user_id}")
        return jsonify({'error': 'Access denied'}), 403

    try:
        # Load the specific session
        from src.session import Session
        try:
            session_obj = Session(session_id)
        except Exception as e:
            logger.warning(f"API: Session {session_id} not found: {e}")
            return jsonify({'error': 'Session not found'}), 404

        # Verify session ownership
        if session_obj.user_id != user_id:
            logger.warning(f"API: User {user_id} trying to get track from session {session_id} owned by {session_obj.user_id}")
            return jsonify({'error': 'Session access denied'}), 403

        # Get dataset with filters applied
        dataset = session_obj.get_dataset()

        # Get a random track from the current pool
        track = dataset.get_random_track()
        if track is None:
            logger.warning(f"API: No tracks available in session {session_id} for user {user_id}")
            return jsonify({'error': 'No tracks available in the current pool'}), 400

        # Update session metadata
        session_obj.update_session_metadata(last_track_id=track['track_id'])

        logger.info(f"API: Served track {track['track_id']} to user {user_id} from session {session_id}")
        return jsonify(track)

    except Exception as e:
        logger.error(f"API: Error getting track for user {user_id} from session {session_id}: {e}")
        return jsonify({'error': 'Failed to get track'}), 500

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)

if __name__ == '__main__':
    # Initialize database
    logger.info("Initializing database...")
    from utils.db import init_db
    init_db('music_app.db')
    
    logger.info("Starting Flask server on port 3001")
    app.run(port=3001, debug=True)