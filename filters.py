"""
Music track filtering functions for the recommendation system.

Each filter function takes a pandas DataFrame (genre_pool) and returns a filtered subset.
Functions implement their own filtering logic based on audio features.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Configuration for filter adjustments
FILTER_CONFIG = {
    'radius': 0.1,  # Standard adjustment radius for most features
    'tempo_radius_factor': 0.15,  # Tempo uses percentage-based radius
    'quantile_threshold': 0.3  # For relative filtering (top/bottom 30%)
}

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
    adjustment = FILTER_CONFIG['radius']
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
    adjustment = FILTER_CONFIG['radius']
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
    adjustment = FILTER_CONFIG['radius']
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
    adjustment = FILTER_CONFIG['radius']
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
    adjustment = FILTER_CONFIG['radius']
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
    adjustment = FILTER_CONFIG['radius']
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
    adjustment = FILTER_CONFIG['radius']
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
    adjustment = FILTER_CONFIG['radius']
    target_max = pool_mean - adjustment
    
    filtered = genre_pool[genre_pool['speechiness'] <= target_max]
    
    logger.debug(f"Speechiness decrease filter: {len(genre_pool)} → {len(filtered)} tracks (threshold: {target_max:.3f})")
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