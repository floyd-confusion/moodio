from flask import Flask, send_from_directory, jsonify, request
import pandas as pd
import random
import numpy as np
import logging

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
        self.df = pd.read_csv('data/dataset.csv')
        self.current_pool = None
        self.original_pool = None
        self.liked_tracks = set()  # Store liked track IDs
    
    # danceability, energy, speechiness, valence, tempo, 
    # loudness, mode(major/minor), acousticness, instrumentalness, liveness

    def get_genres(self):
        genres = []
        for group_title, _ in self.genre_groups.items():
            genres.append(group_title)
        return genres
        
        
    def set_genre_pool(self, genre_group):
        """Set the current pool to tracks from the selected genre group"""
        if genre_group not in self.genre_groups:
            return False
        
        # Get all genres in the selected group
        group_genres = self.genre_groups[genre_group]
        
        # Filter tracks that belong to any of the genres in the group
        self.current_pool = self.df[self.df['track_genre'].isin(group_genres)].copy()
        self.original_pool = self.current_pool.copy()
        
        return len(self.current_pool) > 0
    
    def get_random_track(self):
        """Get a random track from the current pool"""
        if self.current_pool.empty:
            return None
        
        track = self.current_pool.sample(n=1).iloc[0]
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
            return {'error': 'No genre pool selected'}, 400
        
        # Reset to original pool
        self.current_pool = self.original_pool.copy()
        
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
            return {'error': 'Invalid adjustment value. Must be between 0 and 9'}, 400
        
        param, direction = param_info
        
        # Calculate mean value for the parameter
        mean_value = self.current_pool[param].mean()
        
        # Apply adjustment
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
            self.current_pool = self.original_pool.copy()
            return {'error': 'Adjustment too extreme, reset to original pool'}, 200
        
        print(f'Pool adjusted for {param} ({direction})')
        return {
            'message': f'Pool adjusted for {param} ({direction})',
            'remaining_tracks': len(self.current_pool)
        }, 200

    def add_liked_track(self, track_id):
        """Add a track to liked tracks"""
        self.liked_tracks.add(track_id)
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

# Initialize dataset
dataset = Dataset()

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/genres')
def get_genres():
    genres = dataset.get_genres()
    return jsonify(genres)

@app.route('/api/set_genre', methods=['POST'])
def set_genre():
    """Set the genre pool based on the selected genre group"""
    data = request.get_json()
    if not data or 'genre' not in data:
        return jsonify({'error': 'No genre provided'}), 400
    
    genre_group = data['genre']
    if dataset.set_genre_pool(genre_group):
        return jsonify({
            'message': f'Pool set for {genre_group}',
            'count': len(dataset.current_pool)
        })
    return jsonify({'error': 'Invalid genre group or no tracks found'}), 400

@app.route('/api/track')
def get_track():
    """Get a random track from the current pool"""
    track = dataset.get_random_track()
    if track is None:
        return jsonify({'error': 'No tracks available in the current pool'}), 400
    return jsonify(track)

@app.route('/api/adjust_pool', methods=['POST'])
def adjust_pool():
    adjustment = request.json.get('adjustment')
    if adjustment is None:
        return jsonify({'error': 'No adjustment value provided'}), 400
    
    try:
        adjustment = int(adjustment)
        if not 0 <= adjustment <= 9:
            return jsonify({'error': 'Adjustment must be between 0 and 9'}), 400
    except ValueError:
        return jsonify({'error': 'Invalid adjustment value'}), 400
    
    result, status_code = dataset.adjust_pool(adjustment)
    return jsonify(result), status_code

@app.route('/api/likes', methods=['GET', 'POST'])
def handle_likes():
    if request.method == 'POST':
        data = request.get_json()
        if not data or 'track_id' not in data:
            return jsonify({'error': 'No track ID provided'}), 400
        
        track_id = data['track_id']
        if dataset.add_liked_track(track_id):
            return jsonify({'message': 'Track added to likes'})
        return jsonify({'error': 'Failed to add track to likes'}), 400
    
    # GET request
    liked_tracks = dataset.get_liked_tracks()
    return jsonify({'liked_tracks': liked_tracks})

@app.route('/api/track/<track_id>')
def get_track_by_id(track_id):
    """Get track details by ID"""
    track = dataset.get_track_by_id(track_id)
    if track is None:
        return jsonify({'error': 'Track not found'}), 404
    return jsonify(track)

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)

if __name__ == '__main__':
    app.run(port=3001, debug=True)