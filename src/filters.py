"""
Music track filtering functions for the recommendation system.

Each filter function takes a pandas DataFrame (genre_pool) and returns a filtered subset.
Functions implement their own filtering logic based on audio features.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

FILTER_RADIUS = 0.1
FILTER_RADIUS_TEMPO = 0.15
QUANTILE_THRESHOLD = 0.3

def filter_increase_danceability(genre_pool):
    """Filter tracks with higher danceability than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with higher danceability
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_increase_danceability")
        return genre_pool

    pool_mean = genre_pool['danceability'].mean()
    adjustment = FILTER_RADIUS
    target_min = pool_mean + adjustment

    filtered = genre_pool[genre_pool['danceability'] >= target_min]

    logger.debug(f"Danceability increase filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_min:.3f})")
    return filtered

def filter_decrease_danceability(genre_pool):
    """Filter tracks with lower danceability than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with lower danceability
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_decrease_danceability")
        return genre_pool

    pool_mean = genre_pool['danceability'].mean()
    adjustment = FILTER_RADIUS
    target_max = pool_mean - adjustment

    filtered = genre_pool[genre_pool['danceability'] <= target_max]

    logger.debug(f"Danceability decrease filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_max:.3f})")
    return filtered

def filter_increase_valence(genre_pool):
    """Filter tracks with higher valence (more positive/happy) than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with higher valence
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_increase_valence")
        return genre_pool

    pool_mean = genre_pool['valence'].mean()
    adjustment = FILTER_RADIUS
    target_min = pool_mean + adjustment

    filtered = genre_pool[genre_pool['valence'] >= target_min]

    logger.debug(f"Valence increase filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_min:.3f})")
    return filtered

def filter_decrease_valence(genre_pool):
    """Filter tracks with lower valence (more negative/sad) than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with lower valence
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_decrease_valence")
        return genre_pool

    pool_mean = genre_pool['valence'].mean()
    adjustment = FILTER_RADIUS
    target_max = pool_mean - adjustment

    filtered = genre_pool[genre_pool['valence'] <= target_max]

    logger.debug(f"Valence decrease filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_max:.3f})")
    return filtered

def filter_increase_energy(genre_pool):
    """Filter tracks with higher energy than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with higher energy
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_increase_energy")
        return genre_pool

    pool_mean = genre_pool['energy'].mean()
    adjustment = FILTER_RADIUS
    target_min = pool_mean + adjustment

    filtered = genre_pool[genre_pool['energy'] >= target_min]

    logger.debug(f"Energy increase filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_min:.3f})")
    return filtered

def filter_decrease_energy(genre_pool):
    """Filter tracks with lower energy than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with lower energy
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_decrease_energy")
        return genre_pool

    pool_mean = genre_pool['energy'].mean()
    adjustment = FILTER_RADIUS
    target_max = pool_mean - adjustment

    filtered = genre_pool[genre_pool['energy'] <= target_max]

    logger.debug(f"Energy decrease filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_max:.3f})")
    return filtered

def filter_increase_speechiness(genre_pool):
    """Filter tracks with higher speechiness (more vocal/lyrical content) than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with higher speechiness
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_increase_speechiness")
        return genre_pool

    pool_mean = genre_pool['speechiness'].mean()
    adjustment = FILTER_RADIUS
    target_min = pool_mean + adjustment

    filtered = genre_pool[genre_pool['speechiness'] >= target_min]

    logger.debug(f"Speechiness increase filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_min:.3f})")
    return filtered

def filter_decrease_speechiness(genre_pool):
    """Filter tracks with lower speechiness (less vocal/lyrical content) than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with lower speechiness
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_decrease_speechiness")
        return genre_pool

    pool_mean = genre_pool['speechiness'].mean()
    adjustment = FILTER_RADIUS
    target_max = pool_mean - adjustment

    filtered = genre_pool[genre_pool['speechiness'] <= target_max]

    logger.debug(f"Speechiness decrease filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_max:.3f})")
    return filtered

def filter_increase_tempo(genre_pool):
    """Filter tracks with higher tempo than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with higher tempo
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_increase_tempo")
        return genre_pool

    pool_mean = genre_pool['tempo'].mean()
    adjustment = pool_mean * FILTER_RADIUS_TEMPO
    target_min = pool_mean + adjustment

    filtered = genre_pool[genre_pool['tempo'] >= target_min]

    logger.debug(f"Tempo increase filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_min:.1f})")
    return filtered

def filter_decrease_tempo(genre_pool):
    """Filter tracks with lower tempo than the pool average

    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter

    Returns:
        pd.DataFrame: Filtered tracks with lower tempo
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_decrease_tempo")
        return genre_pool

    pool_mean = genre_pool['tempo'].mean()
    adjustment = pool_mean * FILTER_RADIUS_TEMPO
    target_max = pool_mean - adjustment
    
    filtered = genre_pool[genre_pool['tempo'] <= target_max]
    
    logger.debug(f"Tempo decrease filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_max:.1f})")
    return filtered

def filter_progressive_increase_acousticness(genre_pool, application_count=0):
    """Filter tracks with progressively higher acousticness thresholds
    
    Progressive thresholds: 1st=50%+, 2nd=75%+, 3rd+=90%+
    
    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter
        application_count (int): Number of times this filter has been applied (0-based)
        
    Returns:
        pd.DataFrame: Filtered tracks with acousticness above progressive threshold
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_progressive_increase_acousticness")
        return genre_pool
    
    # Progressive thresholds: 50%, 75%, 90%+
    thresholds = [0.5, 0.75, 0.9]
    threshold = thresholds[min(application_count, len(thresholds) - 1)]
    
    filtered = genre_pool[genre_pool['acousticness'] >= threshold]
    
    logger.debug(f"Progressive acousticness increase filter (application #{application_count + 1}): {len(genre_pool)} → {len(filtered)} tracks (threshold: {threshold:.1%})")
    return filtered

def filter_progressive_decrease_acousticness(genre_pool, application_count=0):
    """Filter tracks with progressively lower acousticness thresholds
    
    Progressive thresholds: 1st=50%-, 2nd=25%-, 3rd+=10%-
    
    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter
        application_count (int): Number of times this filter has been applied (0-based)
        
    Returns:
        pd.DataFrame: Filtered tracks with acousticness below progressive threshold
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_progressive_decrease_acousticness")
        return genre_pool
    
    # Progressive thresholds: 50%, 25%, 10%
    thresholds = [0.5, 0.25, 0.1]
    threshold = thresholds[min(application_count, len(thresholds) - 1)]
    
    filtered = genre_pool[genre_pool['acousticness'] <= threshold]
    
    logger.debug(f"Progressive acousticness decrease filter (application #{application_count + 1}): {len(genre_pool)} → {len(filtered)} tracks (threshold: {threshold:.1%})")
    return filtered

def filter_progressive_increase_instrumentalness(genre_pool, application_count=0):
    """Filter tracks with progressively higher instrumentalness thresholds
    
    Progressive thresholds: 1st=50%+, 2nd=75%+, 3rd+=90%+
    
    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter
        application_count (int): Number of times this filter has been applied (0-based)
        
    Returns:
        pd.DataFrame: Filtered tracks with instrumentalness above progressive threshold
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_progressive_increase_instrumentalness")
        return genre_pool
    
    # Progressive thresholds: 50%, 75%, 90%+
    thresholds = [0.5, 0.75, 0.9]
    threshold = thresholds[min(application_count, len(thresholds) - 1)]
    
    filtered = genre_pool[genre_pool['instrumentalness'] >= threshold]
    
    logger.debug(f"Progressive instrumentalness increase filter (application #{application_count + 1}): {len(genre_pool)} → {len(filtered)} tracks (threshold: {threshold:.1%})")
    return filtered

def filter_progressive_decrease_instrumentalness(genre_pool, application_count=0):
    """Filter tracks with progressively lower instrumentalness thresholds
    
    Progressive thresholds: 1st=50%-, 2nd=25%-, 3rd+=10%-
    
    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter
        application_count (int): Number of times this filter has been applied (0-based)
        
    Returns:
        pd.DataFrame: Filtered tracks with instrumentalness below progressive threshold
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_progressive_decrease_instrumentalness")
        return genre_pool
    
    # Progressive thresholds: 50%, 25%, 10%
    thresholds = [0.5, 0.25, 0.1]
    threshold = thresholds[min(application_count, len(thresholds) - 1)]
    
    filtered = genre_pool[genre_pool['instrumentalness'] <= threshold]
    
    logger.debug(f"Progressive instrumentalness decrease filter (application #{application_count + 1}): {len(genre_pool)} → {len(filtered)} tracks (threshold: {threshold:.1%})")
    return filtered

def filter_progressive_increase_liveness(genre_pool, application_count=0):
    """Filter tracks with progressively higher liveness thresholds
    
    Progressive thresholds: 1st=50%+, 2nd=75%+, 3rd+=90%+
    
    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter
        application_count (int): Number of times this filter has been applied (0-based)
        
    Returns:
        pd.DataFrame: Filtered tracks with liveness above progressive threshold
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_progressive_increase_liveness")
        return genre_pool
    
    # Progressive thresholds: 50%, 75%, 90%+
    thresholds = [0.5, 0.75, 0.9]
    threshold = thresholds[min(application_count, len(thresholds) - 1)]
    
    filtered = genre_pool[genre_pool['liveness'] >= threshold]
    
    logger.debug(f"Progressive liveness increase filter (application #{application_count + 1}): {len(genre_pool)} → {len(filtered)} tracks (threshold: {threshold:.1%})")
    return filtered

def filter_progressive_decrease_liveness(genre_pool, application_count=0):
    """Filter tracks with progressively lower liveness thresholds
    
    Progressive thresholds: 1st=50%-, 2nd=25%-, 3rd+=10%-
    
    Args:
        genre_pool (pd.DataFrame): The original genre pool to filter
        application_count (int): Number of times this filter has been applied (0-based)
        
    Returns:
        pd.DataFrame: Filtered tracks with liveness below progressive threshold
    """
    if genre_pool.empty:
        logger.warning("Empty genre pool provided to filter_progressive_decrease_liveness")
        return genre_pool
    
    # Progressive thresholds: 50%, 25%, 10%
    thresholds = [0.5, 0.25, 0.1]
    threshold = thresholds[min(application_count, len(thresholds) - 1)]
    
    filtered = genre_pool[genre_pool['liveness'] <= threshold]
    
    logger.debug(f"Progressive liveness decrease filter (application #{application_count + 1}): {len(genre_pool)} → {len(filtered)} tracks (threshold: {threshold:.1%})")
    return filtered

# Filter registry mapping filter names to functions
FILTER_REGISTRY = {
    'filter_increase_danceability': filter_increase_danceability,
    'filter_decrease_danceability': filter_decrease_danceability,
    'filter_increase_valence': filter_increase_valence,
    'filter_decrease_valence': filter_decrease_valence,
    'filter_increase_energy': filter_increase_energy,
    'filter_decrease_energy': filter_decrease_energy,
    'filter_increase_speechiness': filter_increase_speechiness,
    'filter_decrease_speechiness': filter_decrease_speechiness,
    'filter_increase_tempo': filter_increase_tempo,
    'filter_decrease_tempo': filter_decrease_tempo,
    'filter_progressive_increase_acousticness': filter_progressive_increase_acousticness,
    'filter_progressive_decrease_acousticness': filter_progressive_decrease_acousticness,
    'filter_progressive_increase_instrumentalness': filter_progressive_increase_instrumentalness,
    'filter_progressive_decrease_instrumentalness': filter_progressive_decrease_instrumentalness,
    'filter_progressive_increase_liveness': filter_progressive_increase_liveness,
    'filter_progressive_decrease_liveness': filter_progressive_decrease_liveness,
}

music_filters = {
    # Traditional audio feature filters (0-9)
    "Let's get groovy": "filter_increase_danceability",
    "Take the groove down": "filter_decrease_danceability",
    "Crank up the energy": "filter_increase_energy", 
    "Keep it chill": "filter_decrease_energy",
    "Talk to me — more lyrics": "filter_increase_speechiness",
    "Less chatter, more vibe": "filter_decrease_speechiness",
    "Make it happier": "filter_increase_valence",
    "Go darker, moodier": "filter_decrease_valence",
    "Pick up the pace": "filter_increase_tempo",
    "Slow it down": "filter_decrease_tempo",
    
    # Progressive filters (10-15)
    "More acoustic vibes": "filter_progressive_increase_acousticness",
    "Less acoustic please": "filter_progressive_decrease_acousticness",
    "Go instrumental": "filter_progressive_increase_instrumentalness", 
    "Need more vocals": "filter_progressive_decrease_instrumentalness",
    "Live concert feel": "filter_progressive_increase_liveness",
    "Studio perfection": "filter_progressive_decrease_liveness",
}

def get_filter_function(filter_name):
    """Get a filter function by name
    
    Args:
        filter_name (str): Name of the filter function
        
    Returns:
        callable: Filter function or None if not found
    """
    return FILTER_REGISTRY.get(filter_name)

def list_available_filters():
    """Get list of all available filter names
    
    Returns:
        list: List of available filter function names
    """
    return list(FILTER_REGISTRY.keys())

def create_adjusted_filter(filter_func, multiplier):
    """Create a wrapper that applies radius multiplier to any filter function
    
    Args:
        filter_func (callable): The filter function to wrap
        multiplier (float): Multiplier to apply to filter radius values
        
    Returns:
        callable: Wrapped filter function with adjusted radius
    """
    def adjusted_filter(pool):
        # Declare globals first
        global FILTER_RADIUS, FILTER_RADIUS_TEMPO
        
        # Store original values
        original_radius = FILTER_RADIUS
        original_tempo = FILTER_RADIUS_TEMPO

        # Apply multiplier to all radius variables
        FILTER_RADIUS = original_radius * multiplier
        FILTER_RADIUS_TEMPO = original_tempo * multiplier

        try:
            return filter_func(pool)
        finally:
            # Restore all original values
            FILTER_RADIUS = original_radius
            FILTER_RADIUS_TEMPO = original_tempo
    
    return adjusted_filter