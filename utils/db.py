import sqlite3
import logging
import threading
from contextlib import contextmanager
from typing import Optional, Dict, Any, List, Tuple
import os

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Simple SQLite database client with connection pooling and transaction support."""
    
    def __init__(self, db_path: str = "app.db"):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._initialized = False
        
    def _get_connection(self) -> sqlite3.Connection:
        """
        Get thread-local database connection.
        
        Returns:
            SQLite connection object
        """
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                isolation_level=None  # Autocommit mode
            )
            self._local.connection.row_factory = sqlite3.Row
            
        return self._local.connection
    
    def initialize(self) -> None:
        """Initialize database file and create tables."""
        with self._lock:
            if self._initialized:
                return
                
            # Ensure database directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
            
            # Create the database file and tables
            conn = self._get_connection()
            self._create_tables(conn)
            conn.close()
            
            logger.info(f"Database initialized at {self.db_path}")
            self._initialized = True

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        """Create database tables if they don't exist."""
        
        # Sessions table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Liked tracks table using DataFrame index
        conn.execute('''
            CREATE TABLE IF NOT EXISTS session_liked_tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                track_index INTEGER NOT NULL,
                liked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                UNIQUE(session_id, track_index)
            )
        ''')
        
        # Session state table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS session_state (
                session_id INTEGER PRIMARY KEY,
                current_genre VARCHAR(100),
                fresh_injection_ratio REAL DEFAULT 0.3,
                FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
            )
        ''')
        
        conn.commit()
        logger.info("Database tables created successfully")
    
    def execute(self, query: str, params: Optional[Tuple] = None) -> sqlite3.Cursor:
        """
        Execute a query with optional parameters.
        
        Args:
            query: SQL query string
            params: Query parameters tuple
            
        Returns:
            Cursor object with query results
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
    
    def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[sqlite3.Row]:
        """
        Fetch single row from query.
        
        Args:
            query: SQL query string
            params: Query parameters tuple
            
        Returns:
            Single row as sqlite3.Row or None
        """
        cursor = self.execute(query, params)
        return cursor.fetchone()
    
    def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[sqlite3.Row]:
        """
        Fetch all rows from query.
        
        Args:
            query: SQL query string  
            params: Query parameters tuple
            
        Returns:
            List of rows as sqlite3.Row objects
        """
        cursor = self.execute(query, params)
        return cursor.fetchall()
    
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert data into table.
        
        Args:
            table: Table name
            data: Dictionary of column:value pairs
            
        Returns:
            ID of inserted row
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        cursor = self.execute(query, tuple(data.values()))
        return cursor.lastrowid
    
    def update(self, table: str, data: Dict[str, Any], where: str, params: Optional[Tuple] = None) -> int:
        """
        Update rows in table.
        
        Args:
            table: Table name
            data: Dictionary of column:value pairs to update
            where: WHERE clause (without WHERE keyword)
            params: Parameters for WHERE clause
            
        Returns:
            Number of affected rows
        """
        set_clause = ', '.join([f"{col} = ?" for col in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        
        update_params = list(data.values())
        if params:
            update_params.extend(params)
            
        cursor = self.execute(query, tuple(update_params))
        return cursor.rowcount
    
    def delete(self, table: str, where: str, params: Optional[Tuple] = None) -> int:
        """
        Delete rows from table.
        
        Args:
            table: Table name
            where: WHERE clause (without WHERE keyword)
            params: Parameters for WHERE clause
            
        Returns:
            Number of affected rows
        """
        query = f"DELETE FROM {table} WHERE {where}"
        cursor = self.execute(query, params)
        return cursor.rowcount
    
    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.
        
        Usage:
            with db.transaction():
                db.execute("INSERT ...")
                db.execute("UPDATE ...")
        """
        conn = self._get_connection()
        conn.execute("BEGIN")
        
        try:
            yield
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back: {e}")
            raise
    
    def close_connection(self) -> None:
        """Close thread-local connection."""
        if hasattr(self._local, 'connection'):
            self._local.connection.close()
            delattr(self._local, 'connection')


# Global database instance
db = DatabaseManager()


def init_db(db_path: str = "app.db") -> None:
    """
    Initialize the global database instance.
    
    Args:
        db_path: Path to SQLite database file
    """
    global db
    db = DatabaseManager(db_path)
    db.initialize()


def get_db() -> DatabaseManager:
    """
    Get the global database instance.
    
    Returns:
        DatabaseManager instance
    """
    return db