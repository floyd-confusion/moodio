from flask import Flask, send_from_directory, jsonify, request
import pandas as pd
import random
import numpy as np
import logging
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='public')

class Dataset:
    genre_groups = {
        "Pop & Mainstream": [
            "pop", "power-pop", "dance", "dancehall", "edm", "synth-pop",
            "indie-pop", "j-pop", "k-pop", "mandopop", "cantopop", "latin",
            "latino", "swedish", "party", "pop-film", "show-tunes", "romance"
        ],
        "Rock & Alternative": [
            "rock", "alt-rock", "alternative", "punk", "punk-rock", "hard-rock",
            "metal", "heavy-metal", "metalcore", "death-metal", "black-metal",
            "grindcore", "emo", "grunge", "psych-rock", "rock-n-roll", "rockabilly",
            "british", "indie", "garage", "industrial"
        ],
        "Hip-Hop, R&B & Soul": [
            "hip-hop", "r-n-b", "soul", "funk", "groove", "gospel"
        ],
        "Electronic & Dance": [
            "electronic", "house", "deep-house", "progressive-house", "techno",
            "minimal-techno", "trance", "dubstep", "electro", "detroit-techno",
            "chicago-house", "idm", "drum-and-bass", "dub", "breakbeat", "trip-hop", "club"
        ],
        "Classical, Jazz & Instrumental": [
            "classical", "jazz", "piano", "ambient", "acoustic", "new-age",
            "sleep", "study", "songwriter", "singer-songwriter", "guitar"
        ],
        "World & Regional": [
            "afrobeat", "brazil", "french", "german", "indian", "iranian",
            "j-dance", "j-rock", "malay", "spanish", "turkish", "world-music",
            "samba", "salsa", "forro", "pagode", "mpb", "sertanejo", "tango"
        ],
        "Country, Folk & Roots": [
            "country", "bluegrass", "honky-tonk", "folk"
        ],
        "Niche, Thematic & Other": [
            "anime", "children", "kids", "comedy", "disney", "opera", "happy",
            "sad", "chill", "party", "show-tunes"
        ]
    }
    
    def __init__(self):
        logger.info("Initializing Dataset")
        self.df = pd.read_csv('data/dataset.csv')
        logger.info(f"Loaded dataset with {len(self.df)} tracks")
        self.current_pool = None
        self.original_pool = None
        self.liked_tracks = set()  # Store liked track IDs
        self.adjustment_history = []  # Store adjustment history
    
    # danceability, energy, speechiness, valence, tempo, 
    # loudness, mode(major/minor), acousticness, instrumentalness, liveness

    def get_genres(self):
        genres = []
        for group_title, _ in self.genre_groups.items():
            genres.append(group_title)
        return genres
        
        
    def set_genre_pool(self, genre_group):
        """Set the current pool to tracks from the selected genre group"""
        logger.info(f"Setting genre pool for: {genre_group}")
        if genre_group not in self.genre_groups:
            logger.warning(f"Invalid genre group: {genre_group}")
            return False
        
        # Get all genres in the selected group
        group_genres = self.genre_groups[genre_group]
        
        # Filter tracks that belong to any of the genres in the group
        self.current_pool = self.df[self.df['track_genre'].isin(group_genres)].copy()
        self.original_pool = self.current_pool.copy()
        
        pool_size = len(self.current_pool)
        logger.info(f"Genre pool set with {pool_size} tracks from genres: {group_genres}")
        return pool_size > 0
    
    def get_random_track(self):
        """Get a random track from the current pool, fallback to original pool if empty"""
        # Use current pool if it has tracks, otherwise use original pool (genre pool)
        pool_to_use = self.current_pool if not self.current_pool.empty else self.original_pool
        
        if pool_to_use is None or pool_to_use.empty:
            logger.warning("Attempted to get track from empty pool")
            return None
        
        track = pool_to_use.sample(n=1).iloc[0]
        logger.debug(f"Selected track: {track['track_name']} by {track['artists']}")
        return {
            'track_id': track['track_id'],
            'track_name': track['track_name'],
            'artist_name': track['artists'],
            'genre': track['track_genre'],
            'danceability': track['danceability'],
            'energy': track['energy'],
            'speechiness': track['speechiness'],
            'valence': track['valence'],
            'tempo': track['tempo']
        }
    
    def adjust_pool(self, adjustment):
        if self.original_pool is None:
            logger.error("Adjustment attempted without genre pool selected")
            return {'error': 'No genre pool selected'}, 400
        
        # Parameter mapping (even numbers = less, odd numbers = more)
        param_map = {
            0: ('danceability', 'less'),
            1: ('danceability', 'more'),
            2: ('energy', 'less'),
            3: ('energy', 'more'),
            4: ('speechiness', 'less'),
            5: ('speechiness', 'more'),
            6: ('valence', 'less'),
            7: ('valence', 'more'),
            8: ('tempo', 'less'),
            9: ('tempo', 'more')
        }
        
        # Get the parameter and direction to adjust
        param_info = param_map.get(adjustment)
        if not param_info:
            logger.error(f"Invalid adjustment value: {adjustment}")
            return {'error': 'Invalid adjustment value. Must be between 0 and 9'}, 400
        
        param, direction = param_info
        
        # Record pool size before adjustment
        pool_size_before = len(self.current_pool)
        logger.info(f"Adjusting pool: {param} {direction} (current pool: {pool_size_before} tracks)")
        
        # Calculate mean value for the parameter
        mean_value = self.current_pool[param].mean()
        logger.debug(f"Current mean {param}: {mean_value:.3f}")
        
        # Apply adjustment directly to current pool
        if param == 'tempo':
            # For tempo, adjust by 10% of the mean
            adjustment_range = mean_value * 0.1
            if direction == 'more':
                self.current_pool = self.current_pool[self.current_pool[param] >= mean_value + adjustment_range]
            else:
                self.current_pool = self.current_pool[self.current_pool[param] <= mean_value - adjustment_range]
        else:
            # For other parameters (0-1 range), adjust by 0.1
            if direction == 'more':
                self.current_pool = self.current_pool[self.current_pool[param] >= mean_value + 0.1]
            else:
                self.current_pool = self.current_pool[self.current_pool[param] <= mean_value - 0.1]
        
        # If pool becomes empty, reset to original
        if len(self.current_pool) == 0:
            logger.warning(f"Pool became empty after {param} {direction} adjustment, resetting to original")
            self.current_pool = self.original_pool.copy()
            return {'error': 'Adjustment too extreme, reset to original pool'}, 200
        
        # Log the result
        pool_size_after = len(self.current_pool)
        logger.info(f"Pool adjustment complete: {pool_size_before} → {pool_size_after} tracks")
        
        # Add to adjustment history with stats
        adjustment_record = {
            'timestamp': datetime.datetime.now().isoformat(),
            'adjustment_id': adjustment,
            'parameter': param,
            'direction': direction,
            'pool_size_before': pool_size_before,
            'pool_size_after': pool_size_after,
            'avg_stats_after': {
                'danceability': round(self.current_pool['danceability'].mean(), 3),
                'energy': round(self.current_pool['energy'].mean(), 3),
                'speechiness': round(self.current_pool['speechiness'].mean(), 3),
                'valence': round(self.current_pool['valence'].mean(), 3),
                'tempo': round(self.current_pool['tempo'].mean(), 1)
            }
        }
        self.adjustment_history.append(adjustment_record)
        
        return {
            'message': f'Pool adjusted for {param} ({direction})',
            'remaining_tracks': pool_size_after
        }, 200

    def add_liked_track(self, track_id):
        """Add a track to liked tracks"""
        self.liked_tracks.add(track_id)
        logger.info(f"Track liked: {track_id} (total liked: {len(self.liked_tracks)})")
        return True

    def get_liked_tracks(self):
        """Get all liked tracks"""
        return list(self.liked_tracks)

    def get_track_by_id(self, track_id):
        """Get track details by ID"""
        track = self.df[self.df['track_id'] == track_id]
        if track.empty:
            return None
        
        track = track.iloc[0]
        return {
            'track_id': track['track_id'],
            'track_name': track['track_name'],
            'artist_name': track['artists'],
            'genre': track['track_genre'],
            'danceability': track['danceability'],
            'energy': track['energy'],
            'speechiness': track['speechiness'],
            'valence': track['valence'],
            'tempo': track['tempo']
        }

    def get_pool_stats(self):
        """Get statistics about the current track pool"""
        if self.current_pool is None or self.current_pool.empty:
            return {
                'total_tracks': 0,
                'original_pool_size': 0,
                'current_pool_size': 0,
                'pool_reduction_pct': 0,
                'avg_stats': {}
            }
        
        original_size = len(self.original_pool) if self.original_pool is not None else 0
        current_size = len(self.current_pool)
        reduction_pct = ((original_size - current_size) / original_size * 100) if original_size > 0 else 0
        
        # Calculate average audio features for current pool
        avg_stats = {
            'danceability': round(self.current_pool['danceability'].mean(), 3),
            'energy': round(self.current_pool['energy'].mean(), 3),
            'speechiness': round(self.current_pool['speechiness'].mean(), 3),
            'valence': round(self.current_pool['valence'].mean(), 3),
            'tempo': round(self.current_pool['tempo'].mean(), 1)
        }
        
        return {
            'total_tracks': len(self.df),
            'original_pool_size': original_size,
            'current_pool_size': current_size,
            'pool_reduction_pct': round(reduction_pct, 1),
            'avg_stats': avg_stats
        }

    def get_adjustment_history(self):
        """Get the history of adjustments made by the user"""
        return self.adjustment_history

    def clear_adjustment_history(self):
        """Clear adjustment history and reset to original pool"""
        adjustments_count = len(self.adjustment_history)
        self.adjustment_history = []
        if self.original_pool is not None:
            pool_size_before = len(self.current_pool)
            self.current_pool = self.original_pool.copy()
            pool_size_after = len(self.current_pool)
            logger.info(f"Pool reset: cleared {adjustments_count} adjustments, {pool_size_before} → {pool_size_after} tracks")
        else:
            logger.warning("Attempted to reset pool but no original pool exists")
        return True

# Initialize dataset
dataset = Dataset()

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/genres')
def get_genres():
    logger.info("API: Getting available genres")
    genres = dataset.get_genres()
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
            'message': f'Pool set for {genre_group}',
            'count': len(dataset.current_pool)
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
    liked_tracks = dataset.get_liked_tracks()
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
    """Clear adjustment history and reset pool"""
    logger.info("API: Clearing adjustments and resetting pool")
    if dataset.clear_adjustment_history():
        return jsonify({'message': 'Adjustment history cleared and pool reset'})
    return jsonify({'error': 'Failed to clear adjustment history'}), 400

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)

if __name__ == '__main__':
    logger.info("Starting Flask server on port 3001")
    app.run(port=3001, debug=True)