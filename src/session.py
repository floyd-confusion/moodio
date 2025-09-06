"""
Session management for the music recommendation system.

Each session manages its own Dataset instance and handles persistent storage
of liked tracks and session state through the database.
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from src.dataset import Dataset
from utils.db import get_db

logger = logging.getLogger(__name__)


class Session:
    """
    Manages a single music recommendation session with persistent state.
    
    Handles:
    - Dataset instance management
    - Liked tracks persistence (using DataFrame indices)
    - Session state storage and retrieval
    """
    
    def __init__(self, session_id: int, name: str = None):
        """
        Initialize session with existing ID or create new session.
        
        Args:
            session_id: Existing session ID from database
            name: Session name (for new sessions)
        """
        self.session_id = session_id
        self.name = name
        self.dataset = Dataset()
        self.db = get_db()
        
        # Load session state if it exists
        self._load_state()
        
        logger.info(f"Session {self.session_id} initialized: {self.name}")
    
    @classmethod
    def create_new(cls, name: str) -> 'Session':
        """
        Create a new session in the database.
        
        Args:
            name: Name for the new session
            
        Returns:
            New Session instance
        """
        db = get_db()
        
        # Insert new session
        session_id = db.insert('sessions', {'name': name})
        
        # Initialize session state
        db.insert('session_state', {
            'session_id': session_id,
            'fresh_injection_ratio': 0.3
        })
        
        logger.info(f"Created new session {session_id}: {name}")
        return cls(session_id, name)
    
    def get_dataset(self) -> Dataset:
        """Get the dataset instance for this session."""
        return self.dataset
    
    def add_liked_track(self, track_index: int) -> bool:
        """
        Add a track to liked tracks using its DataFrame index.
        
        Args:
            track_index: Index of track in the dataset DataFrame
            
        Returns:
            True if track was added, False if already liked or error
        """
        try:
            # Check if track exists at this index
            if track_index >= len(self.dataset.df) or track_index < 0:
                logger.warning(f"Invalid track index {track_index} for session {self.session_id}")
                return False
            
            # Insert liked track (will be ignored if already exists due to UNIQUE constraint)
            self.db.execute(
                "INSERT OR IGNORE INTO session_liked_tracks (session_id, track_index) VALUES (?, ?)",
                (self.session_id, track_index)
            )
            
            logger.info(f"Track index {track_index} added to likes for session {self.session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding liked track for session {self.session_id}: {e}")
            return False
    
    def get_liked_tracks(self) -> List[int]:
        """
        Get list of liked track indices for this session.
        
        Returns:
            List of DataFrame indices for liked tracks
        """
        try:
            rows = self.db.fetch_all(
                "SELECT track_index FROM session_liked_tracks WHERE session_id = ? ORDER BY liked_at",
                (self.session_id,)
            )
            
            valid_indices = []
            for row in rows:
                try:
                    # Handle both integer and bytes data
                    track_index = row['track_index']
                    if isinstance(track_index, bytes):
                        logger.warning(f"Found corrupted track_index as bytes: {track_index}, cleaning up...")
                        # Delete the corrupted entry
                        self.db.execute(
                            "DELETE FROM session_liked_tracks WHERE session_id = ? AND track_index = ?",
                            (self.session_id, track_index)
                        )
                        continue
                    
                    valid_indices.append(int(track_index))
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid track_index {track_index} for session {self.session_id}: {e}, skipping")
                    continue
            
            return valid_indices
            
        except Exception as e:
            logger.error(f"Error fetching liked tracks for session {self.session_id}: {e}")
            return []
    
    def get_liked_track_details(self) -> List[Dict[str, Any]]:
        """
        Get detailed information about liked tracks.
        
        Returns:
            List of track dictionaries with full track information
        """
        liked_indices = self.get_liked_tracks()
        tracks = []
        
        for index in liked_indices:
            try:
                if index < len(self.dataset.df):
                    track = self.dataset.df.iloc[index]
                    tracks.append({
                        'track_id': track['track_id'],
                        'track_name': track['track_name'],
                        'artist_name': track['artists'],
                        'genre': track['track_genre'],
                        'index': index
                    })
            except Exception as e:
                logger.warning(f"Error getting details for liked track index {index}: {e}")
                continue
                
        return tracks
    
    def save_state(self) -> bool:
        """
        Save current session state to database.
        
        Returns:
            True if state was saved successfully
        """
        try:
            # Update session timestamp
            self.db.update(
                'sessions',
                {'updated_at': datetime.now().isoformat()},
                'id = ?',
                (self.session_id,)
            )
            
            # Update session state
            current_genre = getattr(self.dataset, 'current_genre_group', None)
            fresh_ratio = getattr(self.dataset, 'fresh_injection_ratio', 0.3)
            
            self.db.execute(
                """INSERT OR REPLACE INTO session_state 
                   (session_id, current_genre, fresh_injection_ratio) 
                   VALUES (?, ?, ?)""",
                (self.session_id, current_genre, fresh_ratio)
            )
            
            logger.debug(f"State saved for session {self.session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving state for session {self.session_id}: {e}")
            return False
    
    def _load_state(self) -> None:
        """Load session state from database."""
        try:
            # Load basic session info
            session_row = self.db.fetch_one(
                "SELECT name FROM sessions WHERE id = ?",
                (self.session_id,)
            )
            
            if session_row:
                self.name = session_row['name']
            
            # Load session state
            state_row = self.db.fetch_one(
                "SELECT current_genre, fresh_injection_ratio FROM session_state WHERE session_id = ?",
                (self.session_id,)
            )
            
            if state_row:
                if state_row['current_genre']:
                    # Restore genre pool if one was set
                    self.dataset.set_genre_pool(state_row['current_genre'])
                
                # Restore fresh injection ratio
                if state_row['fresh_injection_ratio'] is not None:
                    self.dataset.set_fresh_injection_ratio(state_row['fresh_injection_ratio'])
            
            logger.debug(f"State loaded for session {self.session_id}")
            
        except Exception as e:
            logger.error(f"Error loading state for session {self.session_id}: {e}")
    
    def get_session_info(self) -> Dict[str, Any]:
        """
        Get session information.
        
        Returns:
            Dictionary with session details
        """
        return {
            'id': self.session_id,
            'name': self.name,
            'liked_tracks_count': len(self.get_liked_tracks()),
            'current_genre': getattr(self.dataset, 'current_genre_group', None),
            'fresh_injection_ratio': getattr(self.dataset, 'fresh_injection_ratio', 0.3)
        }


def get_all_sessions() -> List[Dict[str, Any]]:
    """
    Get list of all sessions from database.
    
    Returns:
        List of session dictionaries
    """
    try:
        db = get_db()
        rows = db.fetch_all(
            "SELECT id, name, created_at, updated_at FROM sessions ORDER BY updated_at DESC"
        )
        
        sessions = []
        for row in rows:
            sessions.append({
                'id': row['id'],
                'name': row['name'],
                'created_at': row['created_at'],
                'updated_at': row['updated_at']
            })
        
        return sessions
        
    except Exception as e:
        logger.error(f"Error fetching all sessions: {e}")
        return []


def delete_session(session_id: int) -> bool:
    """
    Delete a session and all its associated data.
    
    Args:
        session_id: ID of session to delete
        
    Returns:
        True if session was deleted successfully
    """
    try:
        db = get_db()
        
        with db.transaction():
            # Delete session (CASCADE will handle related tables)
            rows_affected = db.delete('sessions', 'id = ?', (session_id,))
            
        if rows_affected > 0:
            logger.info(f"Session {session_id} deleted successfully")
            return True
        else:
            logger.warning(f"Session {session_id} not found for deletion")
            return False
            
    except Exception as e:
        logger.error(f"Error deleting session {session_id}: {e}")
        return False