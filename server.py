from flask import Flask, send_from_directory, jsonify, request
import pandas as pd
import random

app = Flask(__name__, static_folder='public')

class Dataset:
    def __init__(self):
        self.df = pd.read_csv('data/dataset.csv')
        self.current_pool = None
    
    def get_random_genres(self, n=10):
        # Get unique genres from track_genre column
        unique_genres = self.df['track_genre'].unique()
        # Randomly select n genres
        selected_genres = random.sample(list(unique_genres), n)
        # Convert to list of dictionaries
        return [{'genre': genre} for genre in selected_genres]
    
    def set_genre_pool(self, genre):
        # Filter tracks by genre and store in current_pool
        self.current_pool = self.df[self.df['track_genre'] == genre]
    
    def get_random_track(self):
        if self.current_pool is None or len(self.current_pool) == 0:
            return None
        # Get a random track from the current pool
        random_track = self.current_pool.sample(n=1).iloc[0]
        return {
            'track_id': random_track['track_id'],
            'name': random_track['track_name'],
            'artists': random_track['artists']
        }

# Initialize dataset
dataset = Dataset()

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/genres')
def get_genres():
    genres = dataset.get_random_genres()
    return jsonify(genres)

@app.route('/api/track', methods=['POST'])
def get_track():
    genre = request.json.get('genre')
    if not genre:
        return jsonify({'error': 'No genre provided'}), 400
    
    dataset.set_genre_pool(genre)
    track = dataset.get_random_track()
    if track is None:
        return jsonify({'error': 'No tracks found for this genre'}), 404
    
    return jsonify(track)

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)

if __name__ == '__main__':
    app.run(port=3001, debug=True)