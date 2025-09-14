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
    
    def __init__(self, session_id: int, name: str = None, user_id: int = None):
        """
        Initialize session with existing ID or create new session.
        
        Args:
            session_id: Existing session ID from database
            name: Session name (for new sessions)
            user_id: User ID who owns this session (None for anonymous)
        """
        self.session_id = session_id
        self.name = name
        self.user_id = user_id
        self.dataset = Dataset()
        self.db = get_db()

        # Load session state if it exists
        self._load_state()
        
        logger.info(f"Session {self.session_id} initialized: {self.name} (User: {self.user_id})")
    
    @classmethod
    def create_new(cls, name: str, user_id: int = None) -> 'Session':
        """
        Create a new session in the database.
        
        Args:
            name: Name for the new session
            user_id: User ID who owns this session (None for anonymous)
            
        Returns:
            New Session instance
        """
        db = get_db()
        
        # Insert new session
        session_data = {'name': name}
        if user_id is not None:
            session_data['user_id'] = user_id
            
        session_id = db.insert('sessions', session_data)
        
        # Initialize session state
        db.insert('session_state', {
            'session_id': session_id,
            'fresh_injection_ratio': 0.3
        })
        
        logger.info(f"Created new session {session_id}: {name} (User: {user_id})")
        return cls(session_id, name, user_id)

    def add_liked_track_by_id(self, track_id: str) -> bool:
        """
        Add a track to liked tracks using its track_id.

        Args:
            track_id: Spotify track ID

        Returns:
            True if track was added (new), False if already existed or error
        """
        try:
            # Insert liked track (will be ignored if already exists due to UNIQUE constraint)
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO session_liked_tracks (session_id, track_id) VALUES (?, ?)",
                (self.session_id, track_id)
            )

            # Check if the insert actually added a row (rowcount > 0 means new like)
            was_added = cursor.rowcount > 0

            if was_added:
                logger.info(f"Track {track_id} added to likes for session {self.session_id}")
            else:
                logger.debug(f"Track {track_id} already liked in session {self.session_id}")

            return was_added

        except Exception as e:
            logger.error(f"Error adding liked track {track_id} for session {self.session_id}: {e}")
            return False
    
    def get_liked_track_ids(self) -> List[str]:
        """
        Get list of liked track IDs for this session.

        Returns:
            List of track IDs for liked tracks
        """
        try:
            rows = self.db.fetch_all(
                "SELECT track_id FROM session_liked_tracks WHERE session_id = ? ORDER BY liked_at",
                (self.session_id,)
            )

            track_ids = []
            for row in rows:
                try:
                    track_id = row['track_id']
                    if track_id and isinstance(track_id, str):
                        track_ids.append(track_id)
                    else:
                        logger.warning(f"Invalid track_id {track_id} for session {self.session_id}, skipping")

                except Exception as e:
                    logger.warning(f"Error processing track_id for session {self.session_id}: {e}, skipping")
                    continue

            return track_ids

        except Exception as e:
            logger.error(f"Error fetching liked tracks for session {self.session_id}: {e}")
            return []
    
    def get_liked_track_details(self) -> List[Dict[str, Any]]:
        """
        Get detailed information about liked tracks.

        Returns:
            List of track dictionaries with full track information
        """
        liked_track_ids = self.get_liked_track_ids()
        tracks = []

        for track_id in liked_track_ids:
            try:
                # Find track in dataset by track_id
                track_matches = self.dataset.df[self.dataset.df['track_id'] == track_id]
                if not track_matches.empty:
                    track = track_matches.iloc[0]
                    tracks.append({
                        'track_id': track['track_id'],
                        'track_name': track['track_name'],
                        'artist_name': track['artists'],
                        'genre': track['track_genre']
                    })
                else:
                    logger.warning(f"Liked track {track_id} not found in current dataset for session {self.session_id}")

            except Exception as e:
                logger.warning(f"Error getting details for liked track {track_id}: {e}")
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
            
            # Update session state (only basic session info, no filter state)
            current_genre = getattr(self.dataset, 'current_genre_group', None)

            self.db.execute(
                """INSERT OR REPLACE INTO session_state
                   (session_id, current_genre)
                   VALUES (?, ?, ?)""",
                (self.session_id, current_genre)
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
                "SELECT name, user_id, genre_group, last_track_id FROM sessions WHERE id = ?",
                (self.session_id,)
            )
            
            if session_row:
                self.name = session_row['name']
                self.user_id = session_row['user_id']
                self.dataset.set_genre_pool(session_row['genre_group'])

            logger.debug(f"State loaded for session {self.session_id}")
            
        except Exception as e:
            logger.error(f"Error loading state for session {self.session_id}: {e}")
    
    def update_session_metadata(self, genre_group: str = None, last_track_id: str = None) -> bool:
        """
        Update session metadata in database.
        
        Args:
            genre_group: Genre group for this session
            last_track_id: ID of the last played track
            
        Returns:
            True if update was successful
        """
        try:
            update_data = {'updated_at': datetime.now().isoformat()}
            
            if genre_group is not None:
                update_data['genre_group'] = genre_group
            
            if last_track_id is not None:
                update_data['last_track_id'] = last_track_id
            
            self.db.update(
                'sessions',
                update_data,
                'id = ?',
                (self.session_id,)
            )
            
            logger.debug(f"Session metadata updated for session {self.session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating session metadata for session {self.session_id}: {e}")
            return False

    def get_session_info(self) -> Dict[str, Any]:
        """
        Get session information.
        
        Returns:
            Dictionary with session details
        """
        # Get current session data from database
        session_row = self.db.fetch_one(
            "SELECT name, user_id, genre_group, last_track_id, created_at, updated_at FROM sessions WHERE id = ?",
            (self.session_id,)
        )
        
        # Get filter count from session_filters table
        filter_count_row = self.db.fetch_one(
            "SELECT COUNT(*) as filter_count FROM session_filters WHERE session_id = ?",
            (self.session_id,)
        )
        filter_count = filter_count_row['filter_count'] if filter_count_row else 0

        return {
            'id': self.session_id,
            'name': session_row['name'] if session_row else self.name,
            'user_id': session_row['user_id'] if session_row else self.user_id,
            'genre_group': session_row['genre_group'] if session_row else None,
            'last_track_id': session_row['last_track_id'] if session_row else None,
            'created_at': session_row['created_at'] if session_row else None,
            'updated_at': session_row['updated_at'] if session_row else None,
            'liked_tracks_count': len(self.get_liked_track_ids()),
            'pool_size': 0,  # Will be calculated dynamically when needed
            'adjustment_count': filter_count,
            'current_genre': getattr(self.dataset, 'current_genre_group', None),
            'fresh_injection_ratio': getattr(self.dataset, 'fresh_injection_ratio', 0.3)
        }

    def add_filter(self, filter_type: str, filter_value: int = None) -> bool:
        """
        Add a filter to this session.

        Args:
            filter_type: Type of filter (e.g., 'increase_danceability', 'decrease_energy')
            filter_value: Optional filter value (0-15 for adjustments)

        Returns:
            True if filter was added successfully
        """
        try:
            self.db.execute(
                "INSERT INTO session_filters (session_id, filter_type, filter_value) VALUES (?, ?, ?)",
                (self.session_id, filter_type, filter_value)
            )

            # Update session timestamp
            self.update_session_metadata()

            logger.info(f"Added filter {filter_type} to session {self.session_id}")
            return True

        except Exception as e:
            logger.error(f"Error adding filter to session {self.session_id}: {e}")
            return False

    def get_filters(self) -> List[Dict[str, Any]]:
        """
        Get all filters for this session in chronological order.

        Returns:
            List of filter dictionaries
        """
        try:
            filters = self.db.fetch_all(
                "SELECT filter_type, filter_value, applied_at FROM session_filters WHERE session_id = ? ORDER BY applied_at ASC",
                (self.session_id,)
            )
            filters = [dict(filt) for filt in filters]
            return [
                {
                    'filter_type': row['filter_type'],
                    'filter_value': row['filter_value'],
                    'applied_at': row['applied_at']
                }
                for row in filters
            ]

        except Exception as e:
            logger.error(f"Error getting filters for session {self.session_id}: {e}")
            return []

    def get_dataset(self) -> 'Dataset':
        """
        Get the dataset for this session with all filters applied.
        This reconstructs the dataset state from the filters table each time.

        Returns:
            Dataset instance with filters applied
        """
        try:
            # Load session info to get the genre
            session_row = self.db.fetch_one(
                "SELECT genre_group FROM sessions WHERE id = ?",
                (self.session_id,)
            )

            if session_row and session_row['genre_group']:
                # Set genre pool
                if not self.dataset.set_genre_pool(session_row['genre_group']):
                    logger.error(f"Failed to set genre pool for session {self.session_id}")
                    return self.dataset

                # Get all filters for this session in chronological order
                filters = self.get_filters()

                # If filters are not up-to-date, rebuild playback pool
                if self.dataset.filter_queue != filters:
                    self.dataset.filter_queue = [filter['filter_type'] for filter in filters]
                    self.dataset.rebuild_playback_pool()
            else:
                logger.warning(f"No genre set for session {self.session_id}")

            return self.dataset

        except Exception as e:
            logger.error(f"Error reconstructing dataset for session {self.session_id}: {e}")
            # Return a basic dataset as fallback
            from src.dataset import Dataset
            return Dataset()

    def add_shown_track(self):
        pass

def get_all_sessions(user_id: int = None) -> List[Dict[str, Any]]:
    """
    Get list of sessions from database.
    
    Args:
        user_id: If provided, only return sessions for this user. 
                If None, return all sessions.
    
    Returns:
        List of session dictionaries
    """
    try:
        db = get_db()
        
        if user_id is not None:
            # Get sessions for specific user
            rows = db.fetch_all(
                "SELECT id, name, user_id, created_at, updated_at FROM sessions WHERE user_id = ? ORDER BY updated_at DESC",
                (user_id,)
            )
        else:
            # Get all sessions
            rows = db.fetch_all(
                "SELECT id, name, user_id, created_at, updated_at FROM sessions ORDER BY updated_at DESC"
            )
        
        sessions = []
        for row in rows:
            sessions.append({
                'id': row['id'],
                'name': row['name'],
                'user_id': row['user_id'],
                'created_at': row['created_at'],
                'updated_at': row['updated_at']
            })
        
        return sessions
        
    except Exception as e:
        logger.error(f"Error fetching sessions for user {user_id}: {e}")
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