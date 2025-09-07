from flask import Flask, send_from_directory, jsonify, request, session
import logging
from src.dataset import genre_groups
from src.session import Session, get_all_sessions, delete_session
from src.auth import register_user, authenticate_user, get_user_by_id, AuthError

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='public')
app.secret_key = 'music-recommendation-system-key'  # Enable sessions

# Initialize default session
current_session = None

def get_current_session():
    """Get or create the current session"""
    global current_session
    if current_session is None:
        # Try to get the most recent session or create a new one
        sessions = get_all_sessions()
        if sessions:
            # Use the most recent session
            session_info = sessions[0]
            current_session = Session(session_info['id'], session_info['name'])
            logger.info(f"Loaded existing session: {session_info['name']}")
        else:
            # Create a new default session
            current_session = Session.create_new("Default Session")
            logger.info("Created new default session")
    return current_session

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'auth.html')

@app.route('/genres')
def serve_genres():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/auth')
def serve_auth():
    return send_from_directory(app.static_folder, 'auth.html')

@app.route('/api/genres')
def get_genres():
    logger.info("API: Getting available genres")
    genres =  list(genre_groups.keys())
    return jsonify(genres)

@app.route('/api/set_genre', methods=['POST'])
def set_genre():
    """Set the genre pool based on the selected genre group"""
    data = request.get_json()
    if not data or 'genre' not in data:
        logger.warning("API: set_genre called without genre data")
        return jsonify({'error': 'No genre provided'}), 400
    
    genre_group = data['genre']
    logger.info(f"API: Setting genre pool to {genre_group}")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    if dataset.set_genre_pool(genre_group):
        session_obj.save_state()
        return jsonify({
            'message': f'Genre pool set for {genre_group}',
            'genre_pool_size': len(dataset.genre_pool),
            'playback_pool_size': len(dataset.playback_pool),
            'filters_cleared': True
        })
    return jsonify({'error': 'Invalid genre group or no tracks found'}), 400

@app.route('/api/track')
def get_track():
    """Get a random track from the current pool"""
    logger.debug("API: Getting random track")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    track = dataset.get_random_track()
    if track is None:
        logger.warning("API: No tracks available in current pool")
        return jsonify({'error': 'No tracks available in the current pool'}), 400
    return jsonify(track)

@app.route('/api/adjust_pool', methods=['POST'])
def adjust_pool():
    adjustment = request.json.get('adjustment')
    if adjustment is None:
        logger.warning("API: adjust_pool called without adjustment value")
        return jsonify({'error': 'No adjustment value provided'}), 400
    
    try:
        adjustment = int(adjustment)
        if not 0 <= adjustment <= 15:
            logger.warning(f"API: Invalid adjustment value {adjustment}")
            return jsonify({'error': 'Adjustment must be between 0 and 15'}), 400
    except ValueError:
        logger.warning(f"API: Non-integer adjustment value: {adjustment}")
        return jsonify({'error': 'Invalid adjustment value'}), 400
    
    logger.info(f"API: Processing pool adjustment {adjustment}")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    result, status_code = dataset.adjust_pool(adjustment)
    return jsonify(result), status_code

@app.route('/api/likes', methods=['GET', 'POST'])
def handle_likes():
    session_obj = get_current_session()
    
    if request.method == 'POST':
        data = request.get_json()
        if not data or 'track_id' not in data:
            logger.warning("API: likes POST called without track_id")
            return jsonify({'error': 'No track ID provided'}), 400
        
        track_id = data['track_id']
        logger.info(f"API: Adding track to likes: {track_id}")
        
        # Find the track index in the dataset
        dataset = session_obj.get_dataset()
        track_row = dataset.df[dataset.df['track_id'] == track_id]
        
        if track_row.empty:
            logger.warning(f"API: Track not found: {track_id}")
            return jsonify({'error': 'Track not found'}), 400
        
        track_index = track_row.index[0]
        if session_obj.add_liked_track(track_index):
            return jsonify({'message': 'Track added to likes'})
        return jsonify({'error': 'Failed to add track to likes'}), 400
    
    # GET request
    logger.debug("API: Getting liked tracks")
    liked_track_details = session_obj.get_liked_track_details()
    return jsonify({'liked_tracks': liked_track_details})

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

@app.route('/api/pool_stats')
def get_pool_stats():
    """Get current pool statistics"""
    logger.debug("API: Getting pool statistics")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    stats = dataset.get_pool_stats()
    return jsonify(stats)

@app.route('/api/adjustment_history')
def get_adjustment_history():
    """Get adjustment history"""
    logger.debug("API: Getting adjustment history")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    history = dataset.get_adjustment_history()
    return jsonify({'adjustments': history})

@app.route('/api/clear_adjustments', methods=['POST'])
def clear_adjustments():
    """Clear filter queue and reset pool"""
    logger.info("API: Clearing filters and resetting pool")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    if dataset.clear_adjustment_history():
        return jsonify({
            'message': 'Filter queue cleared and pool reset',
            'genre_pool_size': len(dataset.genre_pool) if dataset.genre_pool is not None else 0,
            'playback_pool_size': len(dataset.playback_pool) if dataset.playback_pool is not None else 0
        })
    return jsonify({'error': 'Failed to clear filters'}), 400

@app.route('/api/filter_queue')
def get_filter_queue():
    """Get current filter queue"""
    logger.debug("API: Getting filter queue")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    return jsonify({
        'filter_queue': dataset.filter_queue,
        'queue_length': len(dataset.filter_queue)
    })


@app.route('/api/pool_tracks')
def get_pool_tracks():
    """Get all tracks in current playback pool"""
    logger.debug("API: Getting all tracks in current pool")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    pool_to_use = dataset.playback_pool if dataset.playback_pool is not None and not dataset.playback_pool.empty else dataset.genre_pool
    
    if pool_to_use is None or pool_to_use.empty:
        logger.warning("API: No tracks available in current pool")
        return jsonify({'error': 'No tracks available in current pool'}), 400
    
    # Convert DataFrame to list of track dictionaries
    tracks = []
    for _, track in pool_to_use.iterrows():
        tracks.append({
            'track_id': track['track_id'],
            'track_name': track['track_name'],
            'artist_name': track['artists'],
            'genre': track['track_genre'],
            'danceability': track['danceability'],
            'energy': track['energy'],
            'speechiness': track['speechiness'],
            'valence': track['valence'],
            'tempo': track['tempo'],
            'acousticness': track['acousticness'],
            'instrumentalness': track['instrumentalness'],
            'liveness': track['liveness']
        })
    
    return jsonify({
        'tracks': tracks,
        'total_count': len(tracks),
        'pool_type': 'playback' if dataset.playback_pool is not None and not dataset.playback_pool.empty else 'genre'
    })

@app.route('/api/fresh_injection_config')
def get_fresh_injection_config():
    """Get current fresh injection configuration"""
    logger.debug("API: Getting fresh injection config")
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    return jsonify(dataset.get_fresh_injection_config())

@app.route('/api/fresh_injection_config', methods=['POST'])
def set_fresh_injection_config():
    """Set fresh injection configuration"""
    data = request.get_json()
    if not data:
        logger.warning("API: fresh_injection_config POST called without data")
        return jsonify({'error': 'No configuration data provided'}), 400
    
    session_obj = get_current_session()
    dataset = session_obj.get_dataset()
    
    if 'fresh_injection_ratio' in data:
        ratio = data['fresh_injection_ratio']
        try:
            ratio = float(ratio)
            if dataset.set_fresh_injection_ratio(ratio):
                session_obj.save_state()
                logger.info(f"API: Fresh injection ratio set to {ratio:.1%}")
                return jsonify({
                    'message': f'Fresh injection ratio set to {ratio:.1%}',
                    'config': dataset.get_fresh_injection_config()
                })
            else:
                return jsonify({'error': 'Invalid ratio. Must be between 0.0 and 1.0'}), 400
        except (ValueError, TypeError):
            logger.warning(f"API: Invalid fresh injection ratio format: {ratio}")
            return jsonify({'error': 'Invalid ratio format. Must be a number between 0.0 and 1.0'}), 400
    
    return jsonify({'error': 'No valid configuration parameters provided'}), 400

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

# Session Management Endpoints

@app.route('/api/sessions', methods=['GET'])
def get_sessions():
    """Get sessions (user's sessions if authenticated, all sessions if anonymous)"""
    logger.debug("API: Getting sessions")
    
    user_id = session.get('user_id')
    
    if user_id:
        # Get user's sessions
        sessions = get_all_sessions(user_id)
        logger.debug(f"API: Returning {len(sessions)} sessions for user {user_id}")
    else:
        # Get all sessions for anonymous users
        sessions = get_all_sessions()
        logger.debug(f"API: Returning {len(sessions)} sessions for anonymous user")
    
    return jsonify({'sessions': sessions})

@app.route('/api/sessions', methods=['POST'])
def create_session():
    """Create a new session"""
    data = request.get_json()
    if not data or 'name' not in data:
        logger.warning("API: create_session called without name")
        return jsonify({'error': 'Session name is required'}), 400
    
    name = data['name'].strip()
    if not name:
        logger.warning("API: create_session called with empty name")
        return jsonify({'error': 'Session name cannot be empty'}), 400
    
    # Get current user if authenticated
    user_id = session.get('user_id')
    
    try:
        new_session = Session.create_new(name, user_id)
        logger.info(f"API: Created new session: {name} (User: {user_id})")
        return jsonify({
            'message': f'Session "{name}" created successfully',
            'session': new_session.get_session_info()
        }), 201
    except Exception as e:
        logger.error(f"API: Error creating session: {e}")
        return jsonify({'error': 'Failed to create session'}), 500

@app.route('/api/sessions/<int:session_id>', methods=['GET'])
def get_session(session_id):
    """Get session details by ID"""
    logger.debug(f"API: Getting session {session_id}")
    try:
        session_obj = Session(session_id)
        
        # Check if user has access to this session
        user_id = session.get('user_id')
        if session_obj.user_id is not None and session_obj.user_id != user_id:
            logger.warning(f"API: User {user_id} denied access to session {session_id} (owner: {session_obj.user_id})")
            return jsonify({'error': 'Access denied to this session'}), 403
            
        return jsonify({'session': session_obj.get_session_info()})
    except Exception as e:
        logger.warning(f"API: Session {session_id} not found: {e}")
        return jsonify({'error': 'Session not found'}), 404

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
        
        # Don't allow deleting the current session
        global current_session
        if current_session and current_session.session_id == session_id:
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

@app.route('/api/current_session', methods=['GET'])
def get_current_session_info():
    """Get current session information"""
    logger.debug("API: Getting current session info")
    session_obj = get_current_session()
    return jsonify({'current_session': session_obj.get_session_info()})

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