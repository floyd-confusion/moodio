import logging
from datetime import datetime
import numpy as np
import pandas as pd
from flask import session
from src.filters import FILTER_REGISTRY, FILTER_RADIUS, create_adjusted_filter, FILTER_RADIUS_TEMPO

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

class Dataset:

    def __init__(self):
        logger.info("Initializing Dataset")
        self.df = pd.read_csv('data/dataset.csv')
        logger.info(f"Loaded dataset with {len(self.df)} tracks")

        # Two-pool system
        self.genre_pool = None  # Starting pool from genre selection (immutable)
        self.playback_pool = None  # Active pool for playing tracks (gets rebuilt)

        # Filter queue system
        self.filter_queue = []  # List of filter operations in order

        # Fresh injection system
        self.fresh_injection_ratio = 0.7  # 30% old tracks, 70% new tracks (default)
        self.pool_size_multiplier = 2.0  # Target size = filter_result_size * multiplier
        self.radius_multiplier_factor = 0.5  # How much to scale radius when reduction > 50%
        
        # Cross-genre expansion system
        self.minimum_pool_threshold = 50  # Minimum tracks required before cross-genre expansion
        self.cross_genre_expansion_ratio = 0.3  # How much of expansion should be cross-genre
        
        # Audio features for calculations
        self.audio_features = ['danceability', 'energy', 'speechiness', 'valence', 'tempo']

        # Track selection strategy
        self.use_average_centered_selection = True  # Avoid extremes by selecting near averages

        self.liked_tracks = set()  # Store liked track IDs
        self.shown_tracks = set()  # Store tracks that have been shown to avoid repetition

        # Legacy compatibility (deprecated)
        self.current_pool = None
        self.original_pool = None
        self.adjustment_history = []

        # danceability, energy, speechiness, valence, tempo,
        # loudness, mode(major/minor), acousticness, instrumentalness, liveness

    def set_genre_pool(self, genre_group):
        """Set the genre pool from the selected genre group and reset system"""
        logger.info(f"Setting genre pool for: {genre_group}")
        if genre_group not in genre_groups:
            logger.warning(f"Invalid genre group: {genre_group}")
            return False

        # Get all genres in the selected group
        group_genres = genre_groups[genre_group]

        # Set genre pool (immutable starting point)
        self.genre_pool = self.df[self.df['track_genre'].isin(group_genres)].copy()

        # Reset filter queue
        self.filter_queue = []

        # Initialize playback pool as copy of genre pool
        self.playback_pool = self.genre_pool.copy()

        # Legacy compatibility
        self.current_pool = self.playback_pool.copy()
        self.original_pool = self.genre_pool.copy()

        pool_size = len(self.genre_pool)
        logger.info(f"Genre pool set with {pool_size} tracks from genres: {group_genres}")
        return pool_size > 0

    def get_random_track(self):
        """Get a track from the current pool using configured selection strategy"""
        # Use playback pool if it has tracks, otherwise use genre pool
        pool_to_use = self.playback_pool if self.playback_pool is not None and not self.playback_pool.empty else self.genre_pool

        if pool_to_use is None or pool_to_use.empty:
            logger.warning("Attempted to get track from empty pool")
            return None

        # Select track based on configured strategy
        if self.use_average_centered_selection:
            track = self._get_average_centered_track(pool_to_use)
        else:
            # For pure random, also exclude shown tracks
            unshown_pool = pool_to_use[~pool_to_use['track_id'].isin(self.shown_tracks)]
            if unshown_pool.empty:
                logger.info(f"All tracks shown, resetting shown tracks history")
                self.shown_tracks.clear()
                unshown_pool = pool_to_use.copy()
            track = unshown_pool.sample(n=1).iloc[0]

        # Record that this track has been shown
        track_id = track['track_id']
        self.shown_tracks.add(track_id)

        logger.debug(
            f"Selected track: {track['track_name']} by {track['artists']} (shown: {len(self.shown_tracks)} total)")
        return {
            'track_id': track_id,
            'track_name': track['track_name'],
            'artist_name': track['artists'],
            'genre': track['track_genre'],
            'danceability': track['danceability'],
            'energy': track['energy'],
            'speechiness': track['speechiness'],
            'valence': track['valence'],
            'tempo': track['tempo']
        }

    def _get_average_centered_track(self, pool):
        """Select a track within FILTER_CONFIG radius of pool averages, excluding shown tracks"""
        if pool.empty:
            return None

        # First, exclude tracks that have already been shown
        unshown_pool = pool[~pool['track_id'].isin(self.shown_tracks)]

        # If all tracks have been shown, reset shown tracks and use full pool
        if unshown_pool.empty:
            logger.info(f"All {len(pool)} tracks have been shown, resetting shown tracks history")
            self.shown_tracks.clear()
            unshown_pool = pool.copy()

        logger.debug(f"Track selection pool: {len(unshown_pool)} unshown tracks (out of {len(pool)} total)")

        # Calculate pool averages
        pool_averages = {}
        for feature in self.audio_features:
            pool_averages[feature] = unshown_pool[feature].mean()

        # Filter tracks that are within radius of averages (same logic as filters use)
        radius = FILTER_RADIUS  # 0.1
        tempo_radius_factor = FILTER_RADIUS_TEMPO # 0.15

        candidates = unshown_pool.copy()

        # Apply radius constraints for each feature
        for feature in self.audio_features:
            avg_value = pool_averages[feature]

            if feature == 'tempo':
                # Use percentage-based radius for tempo (same as filters)
                tempo_radius = avg_value * tempo_radius_factor
                min_val = avg_value - tempo_radius
                max_val = avg_value + tempo_radius
            else:
                # Use fixed radius for 0-1 scale features
                min_val = avg_value - radius
                max_val = avg_value + radius

            # Filter candidates to stay within radius
            candidates = candidates[
                (candidates[feature] >= min_val) &
                (candidates[feature] <= max_val)
                ]

            logger.debug(f"After {feature} radius filter: {len(candidates)} tracks remaining")

        # If no tracks within radius, fall back to closest unshown tracks
        if candidates.empty:
            logger.debug("No unshown tracks within radius, selecting closest to averages")
            distances = []
            for idx, track in unshown_pool.iterrows():
                distance = 0
                for feature in self.audio_features:
                    if feature == 'tempo':
                        # Normalize tempo for distance calculation
                        track_norm = (track[feature] - 60) / (200 - 60)
                        avg_norm = (pool_averages[feature] - 60) / (200 - 60)
                        distance += (track_norm - avg_norm) ** 2
                    else:
                        distance += (track[feature] - pool_averages[feature]) ** 2
                distances.append((idx, np.sqrt(distance)))

            # Select from closest 10% of unshown tracks as fallback
            distances.sort(key=lambda x: x[1])
            fallback_size = max(1, len(distances) // 10)
            candidate_indices = [idx for idx, dist in distances[:fallback_size]]
            selected_idx = np.random.choice(candidate_indices)
            track = unshown_pool.loc[selected_idx]
            logger.debug(f"Fallback selection: closest unshown track distance {distances[0][1]:.3f}")
        else:
            # Random selection from unshown tracks within radius
            track = candidates.sample(n=1).iloc[0]
            logger.debug(f"Radius-constrained selection: {len(candidates)} unshown candidates within radius")

        return track

    def adjust_pool(self, adjustment):
        """Add a filter to the queue and rebuild the playback pool"""
        if self.genre_pool is None:
            logger.error("Adjustment attempted without genre pool selected")
            return {'error': 'No genre pool selected'}, 400

        # Parameter mapping (even numbers = decrease, odd numbers = increase)
        filter_map = {
            0: 'filter_decrease_danceability',
            1: 'filter_increase_danceability',
            2: 'filter_decrease_energy',
            3: 'filter_increase_energy',
            4: 'filter_decrease_speechiness',
            5: 'filter_increase_speechiness',
            6: 'filter_decrease_valence',
            7: 'filter_increase_valence',
            8: 'filter_decrease_tempo',
            9: 'filter_increase_tempo'
        }

        # Get the filter function name
        filter_name = filter_map.get(adjustment)
        if not filter_name:
            logger.error(f"Invalid adjustment value: {adjustment}")
            return {'error': 'Invalid adjustment value. Must be between 0 and 9'}, 400

        # Check if filter function exists
        if filter_name not in FILTER_REGISTRY:
            logger.error(f"Filter function not found: {filter_name}")
            return {'error': f'Filter not implemented: {filter_name}'}, 500

        # Handle contradicting filters - either add new filter or drop both
        filter_record = {
            'timestamp': datetime.now().isoformat(),
            'adjustment_id': adjustment,
            'filter_name': filter_name
        }
        self.filter_queue = self._remove_contradicting_filter(self.filter_queue, adjustment, filter_record)

        # Rebuild playback pool by applying all filters in sequence
        pool_size_before = len(self.playback_pool) if self.playback_pool is not None else 0
        self._rebuild_playback_pool()
        pool_size_after = len(self.playback_pool)

        logger.info(f"Filter applied: {filter_name} -> {pool_size_before} → {pool_size_after} tracks")

        # Legacy compatibility
        self.current_pool = self.playback_pool.copy()

        # Add to legacy adjustment history
        param_direction = filter_name.replace('filter_', '').replace('_', ' ')
        adjustment_record = {
            'timestamp': filter_record['timestamp'],
            'adjustment_id': adjustment,
            'parameter': param_direction.split(' ')[1],  # e.g., 'danceability'
            'direction': param_direction.split(' ')[0],  # e.g., 'increase'
            'pool_size_before': pool_size_before,
            'pool_size_after': pool_size_after,
            'avg_stats_after': self._get_pool_averages()
        }
        self.adjustment_history.append(adjustment_record)

        return {
            'message': f'Filter applied: {filter_name}',
            'remaining_tracks': pool_size_after,
            'filters_in_queue': len(self.filter_queue)
        }, 200

    def _calculate_reduction_rate(self, pool_before, pool_after):
        """Calculate the reduction rate from filter application"""
        if pool_before == 0:
            return 0.0
        return (pool_before - pool_after) / pool_before

    def _mix_pools(self, old_pool, new_filtered_result):
        """Mix old pool (30%) with new filtered result (70%) based on filtered result size"""
        if new_filtered_result.empty:
            logger.warning("Cannot mix pools: new filtered result is empty")
            return old_pool if old_pool is not None and not old_pool.empty else pd.DataFrame()

        # Base the mixing size on the filtered result size
        filtered_pool_size = len(new_filtered_result)
        current_ratio = self.get_fresh_injection_ratio()  # Default 0.3 (30% old)

        # Calculate track counts using direct ratios
        old_track_count = int(filtered_pool_size * current_ratio)  # 30% from old pool
        new_track_count = filtered_pool_size - old_track_count  # 70% from new result

        # Ensure we don't exceed available tracks
        old_track_count = min(old_track_count, len(old_pool) if old_pool is not None and not old_pool.empty else 0)
        new_track_count = min(new_track_count, len(new_filtered_result))

        # If we can't get enough old tracks, add more new tracks to maintain total
        if old_track_count < int(filtered_pool_size * current_ratio):
            shortfall = int(filtered_pool_size * current_ratio) - old_track_count
            new_track_count = min(new_track_count + shortfall, len(new_filtered_result))

        logger.debug(
            f"Pool mixing: {old_track_count} old + {new_track_count} new = {old_track_count + new_track_count} total")

        # Sample tracks from both sources
        mixed_tracks = []

        # Add old tracks
        if old_track_count > 0 and old_pool is not None and not old_pool.empty:
            old_tracks = old_pool.sample(n=min(old_track_count, len(old_pool)))
            mixed_tracks.append(old_tracks)
            logger.debug(f"Added {len(old_tracks)} old tracks")

        # Add new tracks
        if new_track_count > 0:
            new_tracks = new_filtered_result.sample(n=min(new_track_count, len(new_filtered_result)))
            mixed_tracks.append(new_tracks)
            logger.debug(f"Added {len(new_tracks)} new tracks")

        # Combine and shuffle the final result
        if mixed_tracks:
            mixed_pool = pd.concat(mixed_tracks, ignore_index=True)
            # Final shuffle to mix old and new tracks together
            mixed_pool = mixed_pool.sample(frac=1.0).reset_index(drop=True)
            logger.debug(f"Pool mixing complete: {len(mixed_pool)} tracks (ratio {current_ratio:.1%})")
            return mixed_pool
        else:
            # Fallback to new filter result if mixing failed
            logger.warning("Pool mixing failed, using new filter result only")
            return new_filtered_result

    def _rebuild_playback_pool(self):
        """Rebuild playback pool by applying filters one-by-one with mixing after each"""
        if self.genre_pool is None:
            logger.warning("Cannot rebuild playback pool: no genre pool set")
            return

        if len(self.filter_queue) == 0:
            # No filters: playback pool = genre pool
            self.playback_pool = self.genre_pool.copy()
            logger.info(f"No filters. Playback pool set to genre pool: {len(self.playback_pool)} tracks")
            return

        # Start with genre pool for the first filter
        current_pool = self.genre_pool.copy()

        # Apply each filter one-by-one with mixing after each
        for i, filter_record in enumerate(self.filter_queue):
            filter_name = filter_record['filter_name']
            filter_func = FILTER_REGISTRY.get(filter_name)

            if not filter_func:
                logger.error(f"Filter function not found: {filter_name}")
                continue

            # Test filter to calculate reduction rate
            pool_before = len(current_pool)
            test_result = filter_func(current_pool.copy())
            pool_after = len(test_result)
            reduction_rate = self._calculate_reduction_rate(pool_before, pool_after)

            # Apply filter with adjusted radius if reduction > 50%
            multiplier = self.radius_multiplier_factor if reduction_rate > 0.5 else 1.0
            adjusted_filter = create_adjusted_filter(filter_func, multiplier)
            filtered_result = adjusted_filter(current_pool.copy())
            logger.debug(f"Applied {filter_name} with {reduction_rate:.1%} reduction rate using {multiplier}x radius multiplier")

            if filtered_result.empty:
                logger.warning(f"Pool became empty after filter {i + 1}: {filter_name}, skipping remaining filters")
                break

            # Mix the filtered result with the previous pool (30% old + 70% new)
            # For first filter, mix with genre_pool; for subsequent, mix with current playback_pool
            old_pool_for_mixing = self.genre_pool if i == 0 else self.playback_pool
            self.playback_pool = filtered_result

            # Update current_pool for next iteration
            current_pool = self.playback_pool.copy()

            logger.info(f"Filter {i + 1} complete: {filter_name} -> mixed pool has {len(self.playback_pool)} tracks")

        # Check if we need cross-genre expansion after all filters
        if (self.playback_pool is not None and 
            len(self.playback_pool) < self.minimum_pool_threshold):
            self._expand_with_cross_genre()

        # Ensure we have a valid playback pool
        if self.playback_pool is None or self.playback_pool.empty:
            logger.warning("Playback pool is empty after all filters, falling back to genre pool")
            self.playback_pool = self.genre_pool.copy()

    def _expand_with_cross_genre(self):
        """Expand current pool using dataset averages within filter radius from total pool"""
        needed_tracks = self.minimum_pool_threshold - len(self.playback_pool)
        
        if needed_tracks <= 0:
            return
        
        logger.info(f"Expanding pool to minimum {self.minimum_pool_threshold} tracks using dataset averages")
        
        # Select tracks from entire dataset within radius of dataset averages
        dataset_averages = self._get_pool_averages(self.df)
        selected_tracks = self._select_tracks_near_averages(self.df, needed_tracks, dataset_averages)
        
        if not selected_tracks.empty:
            # Add selected tracks to playback pool
            self.playback_pool = pd.concat([self.playback_pool, selected_tracks], ignore_index=True)
            # Shuffle to mix new tracks with existing tracks
            self.playback_pool = self.playback_pool.sample(frac=1.0).reset_index(drop=True)
            
            logger.info(f"Cross-genre expansion complete: added {len(selected_tracks)} tracks near dataset averages (final size: {len(self.playback_pool)})")
        else:
            logger.warning("No tracks found within radius of dataset averages")
    
    def _select_tracks_near_averages(self, pool, needed_count, target_averages):
        """Select tracks within filter radius of target averages"""
        candidates = pool.copy()
        
        # Apply radius constraints for each feature (same logic as average-centered selection)
        for feature in self.audio_features:
            if feature not in target_averages:
                continue
                
            avg_value = target_averages[feature]
            
            if feature == 'tempo':
                # Use percentage-based radius for tempo
                tempo_radius = avg_value * FILTER_RADIUS_TEMPO
                min_val = avg_value - tempo_radius
                max_val = avg_value + tempo_radius
            else:
                # Use fixed radius for 0-1 scale features
                min_val = avg_value - FILTER_RADIUS
                max_val = avg_value + FILTER_RADIUS
            
            # Filter candidates to stay within radius
            candidates = candidates[
                (candidates[feature] >= min_val) &
                (candidates[feature] <= max_val)
            ]

        
        # Random sample from candidates within radius
        if not candidates.empty:
            sample_size = min(needed_count, len(candidates))
            selected_tracks = candidates.sample(n=sample_size)
            logger.debug(f"Selected {len(selected_tracks)} tracks within radius of dataset averages")
            return selected_tracks
        else:
            logger.warning("No tracks found within radius, returning empty DataFrame")
            return pd.DataFrame()

    def _get_pool_averages(self, pool=None):
        """Get average audio features for the specified pool (defaults to current playback pool)"""
        target_pool = pool if pool is not None else self.playback_pool
        
        if target_pool is None or target_pool.empty:
            return {}

        averages = {}
        for feature in self.audio_features:
            if feature == 'tempo':
                averages[feature] = round(target_pool[feature].mean(), 1)
            else:
                averages[feature] = round(target_pool[feature].mean(), 3)
        
        return averages

    def add_liked_track(self, track_id):
        """Add a track to liked tracks"""
        self.liked_tracks.add(track_id)
        logger.info(f"Track liked: {track_id} (total liked: {len(self.liked_tracks)})")
        return True

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
        """Get statistics about the current track pools"""
        if self.playback_pool is None or self.playback_pool.empty:
            return {
                'total_tracks': len(self.df) if self.df is not None else 0,
                'genre_pool_size': len(self.genre_pool) if self.genre_pool is not None else 0,
                'playback_pool_size': 0,
                'pool_reduction_pct': 0,
                'filters_applied': len(self.filter_queue),
                'avg_stats': {}
            }

        genre_size = len(self.genre_pool) if self.genre_pool is not None else 0
        playback_size = len(self.playback_pool)
        reduction_pct = ((genre_size - playback_size) / genre_size * 100) if genre_size > 0 else 0

        avg_stats = self._get_pool_averages()

        return {
            'total_tracks': len(self.df),
            'genre_pool_size': genre_size,
            'playback_pool_size': playback_size,
            'pool_reduction_pct': round(reduction_pct, 1),
            'filters_applied': len(self.filter_queue),
            'tracks_shown': len(self.shown_tracks),
            'avg_stats': avg_stats
        }

    def get_adjustment_history(self):
        """Get the history of adjustments made by the user"""
        return self.adjustment_history

    def clear_adjustment_history(self):
        """Clear filter queue, shown tracks, and reset to genre pool"""
        filters_count = len(self.filter_queue)
        adjustments_count = len(self.adjustment_history)
        shown_count = len(self.shown_tracks)

        # Clear filter queue, adjustment history, and shown tracks
        self.filter_queue = []
        self.adjustment_history = []
        self.shown_tracks.clear()

        if self.genre_pool is not None:
            pool_size_before = len(self.playback_pool) if self.playback_pool is not None else 0
            self.playback_pool = self.genre_pool.copy()
            self.current_pool = self.playback_pool.copy()  # Legacy compatibility
            pool_size_after = len(self.playback_pool)
            logger.info(
                f"System reset: removed {filters_count} filters, cleared {shown_count} shown tracks, {pool_size_before} → {pool_size_after} tracks")
        else:
            logger.warning("Attempted to reset pool but no genre pool exists")

        return True

    def clear_shown_tracks(self):
        """Clear only the shown tracks history"""
        shown_count = len(self.shown_tracks)
        self.shown_tracks.clear()
        logger.info(f"Cleared {shown_count} shown tracks")
        return True

    def get_fresh_injection_ratio(self):
        """Get fresh injection ratio, preferring session value over default"""
        return session.get('fresh_injection_ratio', self.fresh_injection_ratio)

    def set_fresh_injection_ratio(self, ratio):
        """Set fresh injection ratio for both instance and session"""
        if 0.0 <= ratio <= 1.0:
            self.fresh_injection_ratio = ratio
            session['fresh_injection_ratio'] = ratio
            logger.info(f"Fresh injection ratio set to {ratio:.1%}")
            return True
        else:
            logger.error(f"Invalid fresh injection ratio: {ratio}. Must be between 0.0 and 1.0")
            return False

    def get_fresh_injection_config(self):
        """Get current fresh injection configuration"""
        return {
            'fresh_injection_ratio': self.get_fresh_injection_ratio(),
            'pool_size_multiplier': self.pool_size_multiplier,
            'description': {
                'fresh_injection_ratio': f'{self.get_fresh_injection_ratio():.1%} old tracks, {1 - self.get_fresh_injection_ratio():.1%} new tracks',
                'pool_size_multiplier': f'Target size = filter_result_size × {self.pool_size_multiplier}'
            }
        }

    def get_selection_config(self):
        """Get current track selection configuration"""
        return {
            'use_average_centered_selection': self.use_average_centered_selection,
            'description': {
                'strategy': 'Average-centered (avoids extremes)' if self.use_average_centered_selection else 'Pure random',
                'explanation': 'Selects tracks closer to pool averages to avoid extreme outliers' if self.use_average_centered_selection else 'Completely random selection from pool'
            }
        }

    def set_selection_strategy(self, use_average_centered):
        """Set track selection strategy"""
        if isinstance(use_average_centered, bool):
            self.use_average_centered_selection = use_average_centered
            strategy = 'average-centered' if use_average_centered else 'random'
            logger.info(f"Track selection strategy set to: {strategy}")
            return True
        else:
            logger.error(f"Invalid selection strategy: {use_average_centered}. Must be boolean")
            return False

    def _remove_contradicting_filter(self, filter_queue, new_adjustment_id, new_filter_record):
        """Return filter queue with contradicting filter handled - either add new filter or drop both"""
        contradicting_id = new_adjustment_id ^ 1  # XOR flips even/odd pairs
        
        # Check if contradicting filter exists
        has_contradiction = any(f['adjustment_id'] == contradicting_id for f in filter_queue)
        
        if has_contradiction:
            # Drop both - remove contradicting filter and don't add new one
            cleaned_queue = [f for f in filter_queue if f['adjustment_id'] != contradicting_id]
            logger.info(f"Contradiction detected: dropped existing filter (adjustment_{contradicting_id}) and not adding new filter (adjustment_{new_adjustment_id})")
            return cleaned_queue
        else:
            # No contradiction - add new filter
            return filter_queue + [new_filter_record]

