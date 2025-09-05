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
}

music_filters = {
    "Like this artist!": "filter_more_from_artist",
    "Try someone new": "filter_new_artist",
    "More from this album?": "filter_more_from_album",
    "Let’s get groovy": "filter_increase_danceability",
    "Take the groove down": "filter_decrease_danceability",
    "Crank up the energy": "filter_increase_energy",
    "Keep it chill": "filter_decrease_energy",
    "Talk to me — more lyrics": "filter_increase_speechiness",
    "Less chatter, more vibe": "filter_decrease_speechiness",
    "Play more acoustic stuff": "filter_include_acoustic",
    "No unplugged right now": "filter_exclude_acoustic",
    "Go full instrumental": "filter_include_instrumental",
    "Bring back the vocals": "filter_exclude_instrumental",
    "Give me that live feel": "filter_include_live",
    "Studio only, please": "filter_exclude_live",
    "Make it happier": "filter_increase_valence",
    "Go darker, moodier": "filter_decrease_valence",
    "Pick up the pace": "filter_increase_tempo",
    "Slow it down": "filter_decrease_tempo",
    "Keep the genre vibe": "filter_maintain_genre",
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