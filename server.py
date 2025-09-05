from flask import Flask, send_from_directory, jsonify, request, session
import logging
from src.dataset import Dataset, genre_groups

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='public')
app.secret_key = 'music-recommendation-system-key'  # Enable sessions

# Initialize dataset
dataset = Dataset()

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

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
    if dataset.set_genre_pool(genre_group):
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
        if not 0 <= adjustment <= 9:
            logger.warning(f"API: Invalid adjustment value {adjustment}")
            return jsonify({'error': 'Adjustment must be between 0 and 9'}), 400
    except ValueError:
        logger.warning(f"API: Non-integer adjustment value: {adjustment}")
        return jsonify({'error': 'Invalid adjustment value'}), 400
    
    logger.info(f"API: Processing pool adjustment {adjustment}")
    result, status_code = dataset.adjust_pool(adjustment)
    return jsonify(result), status_code

@app.route('/api/likes', methods=['GET', 'POST'])
def handle_likes():
    if request.method == 'POST':
        data = request.get_json()
        if not data or 'track_id' not in data:
            logger.warning("API: likes POST called without track_id")
            return jsonify({'error': 'No track ID provided'}), 400
        
        track_id = data['track_id']
        logger.info(f"API: Adding track to likes: {track_id}")
        if dataset.add_liked_track(track_id):
            return jsonify({'message': 'Track added to likes'})
        return jsonify({'error': 'Failed to add track to likes'}), 400
    
    # GET request
    logger.debug("API: Getting liked tracks")
    liked_tracks = list(dataset.liked_tracks)
    return jsonify({'liked_tracks': liked_tracks})

@app.route('/api/track/<track_id>')
def get_track_by_id(track_id):
    """Get track details by ID"""
    logger.debug(f"API: Getting track by ID: {track_id}")
    track = dataset.get_track_by_id(track_id)
    if track is None:
        logger.warning(f"API: Track not found: {track_id}")
        return jsonify({'error': 'Track not found'}), 404
    return jsonify(track)

@app.route('/api/pool_stats')
def get_pool_stats():
    """Get current pool statistics"""
    logger.debug("API: Getting pool statistics")
    stats = dataset.get_pool_stats()
    return jsonify(stats)

@app.route('/api/adjustment_history')
def get_adjustment_history():
    """Get adjustment history"""
    logger.debug("API: Getting adjustment history")
    history = dataset.get_adjustment_history()
    return jsonify({'adjustments': history})

@app.route('/api/clear_adjustments', methods=['POST'])
def clear_adjustments():
    """Clear filter queue and reset pool"""
    logger.info("API: Clearing filters and resetting pool")
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
    return jsonify({
        'filter_queue': dataset.filter_queue,
        'queue_length': len(dataset.filter_queue)
    })


@app.route('/api/pool_tracks')
def get_pool_tracks():
    """Get all tracks in current playback pool"""
    logger.debug("API: Getting all tracks in current pool")
    
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
            'tempo': track['tempo']
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
    return jsonify(dataset.get_fresh_injection_config())

@app.route('/api/fresh_injection_config', methods=['POST'])
def set_fresh_injection_config():
    """Set fresh injection configuration"""
    data = request.get_json()
    if not data:
        logger.warning("API: fresh_injection_config POST called without data")
        return jsonify({'error': 'No configuration data provided'}), 400
    
    if 'fresh_injection_ratio' in data:
        ratio = data['fresh_injection_ratio']
        try:
            ratio = float(ratio)
            if dataset.set_fresh_injection_ratio(ratio):
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