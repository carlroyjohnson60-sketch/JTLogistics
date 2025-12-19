"""Minimal MySQL client wrapper (optional). If DB disabled in config, this becomes a no-op."""
import mysql.connector
import logging
from mysql.connector import Error


class DBClient:
    """Database client with error handling."""

    def __init__(self, cfg: dict):
        self.cfg = cfg or {}
        self.enabled = bool(self.cfg.get('enabled'))
        self.conn = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """Connect to MySQL database with error handling.
        
        Raises:
            mysql.connector.Error: If connection fails
        """
        if not self.enabled:
            self.logger.debug("Database disabled in configuration")
            return
        
        try:
            self.logger.info(f"Connecting to database: {self.cfg.get('host')}:{self.cfg.get('port', 3306)}")
            self.conn = mysql.connector.connect(
                host=self.cfg.get('host'),
                port=self.cfg.get('port', 3306),
                user=self.cfg.get('user'),
                password=self.cfg.get('password'),
                database=self.cfg.get('database'),
            )
            self.logger.info("Database connection established")
        except Error as e:
            self.logger.error(f"Database connection failed: {e}")
            raise

    def insert_response(self, flow_key: str, file_name: str, payload: dict, response_text: str):
        """Insert API response into database with error handling.
        
        Args:
            flow_key: Flow identifier
            file_name: Source file name
            payload: Request payload
            response_text: Response text
        """
        if not self.enabled or not self.conn:
            self.logger.debug("Database insert skipped (disabled or not connected)")
            return
        
        try:
            cursor = self.conn.cursor()
            # minimal table assumptions; user should adapt to their schema
            sql = """
            INSERT INTO flow_responses (flow_key, file_name, payload_json, response_text, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            """
            cursor.execute(sql, (flow_key, file_name, str(payload), response_text))
            self.conn.commit()
            self.logger.info(f"Response inserted for flow={flow_key}, file={file_name}")
        
        except Error as e:
            self.logger.error(f"Database insert failed: {e}")
            if self.conn:
                self.conn.rollback()
        except Exception as e:
            self.logger.error(f"Unexpected error during database insert: {e}")
            if self.conn:
                self.conn.rollback()

    def is_connected(self) -> bool:
        """Check if database is connected.
        
        Returns:
            bool: True if connected, False otherwise
        """
        return self.conn is not None and self.conn.is_connected()
