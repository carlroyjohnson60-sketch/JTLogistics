import os
import yaml
import logging


class ConfigLoader:
    """Load and validate configuration from YAML file."""

    def __init__(self, path=None):
        self.logger = logging.getLogger(__name__)
        self.path = path or os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
        self.data = self._load()

    def _load(self):
        """Load configuration from YAML file with error handling.
        
        Returns:
            dict: Parsed YAML configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file has invalid YAML
        """
        try:
            p = os.path.abspath(self.path)
            if not os.path.exists(p):
                raise FileNotFoundError(f"Config file not found: {p}")
            
            with open(p, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if not data:
                raise ValueError(f"Config file is empty: {p}")
            
            self.logger.info(f"Configuration loaded successfully from {p}")
            return data
        
        except FileNotFoundError as e:
            self.logger.error(f"Configuration file not found: {e}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Invalid YAML in config file: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading config: {e}")
            raise

    def get_global(self, key, default=None):
        """Get global configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        try:
            return self.data.get('globals', {}).get(key, default)
        except Exception as e:
            self.logger.warning(f"Error getting global config '{key}': {e}")
            return default

    def get_db(self):
        """Get database configuration.
        
        Returns:
            dict: Database configuration
        """
        try:
            db_config = self.data.get('db', {})
            if not db_config:
                self.logger.warning("No database configuration found")
            return db_config
        except Exception as e:
            self.logger.error(f"Error getting database config: {e}")
            return {}

    def get_email(self):
        """Get email configuration.
        
        Returns:
            dict: Email configuration
        """
        try:
            email_config = self.data.get('email', {})
            if not email_config:
                self.logger.warning("No email configuration found")
            return email_config
        except Exception as e:
            self.logger.error(f"Error getting email config: {e}")
            return {}

    def get_auth(self):
        """Get OAuth2 authentication configuration.
        
        Returns:
            dict: Authentication configuration
        """
        try:
            auth_config = self.data.get('auth', {})
            if not auth_config:
                self.logger.warning("No authentication configuration found")
            return auth_config
        except Exception as e:
            self.logger.error(f"Error getting authentication config: {e}")
            return {}

    def get_flow(self, partner: str, direction: str, flow_name: str):
        """Get specific flow configuration.
        
        Args:
            partner: Partner name
            direction: 'inbound' or 'outbound'
            flow_name: Flow name
            
        Returns:
            dict: Flow configuration
            
        Raises:
            KeyError: If partner, direction, or flow not found
        """
        try:
            partners = self.data.get('partners', {})
            if not partners:
                raise KeyError("No partners found in config")
            
            p = partners.get(partner)
            if not p:
                raise KeyError(f"Partner not found in config: {partner}")
            
            d = p.get(direction)
            if not d:
                raise KeyError(f"Direction '{direction}' not found for partner {partner}")
            
            flow = d.get(flow_name)
            if not flow:
                raise KeyError(f"Flow '{flow_name}' not found for {partner}.{direction}")
            
            self.logger.info(f"Flow config loaded: {partner}/{direction}/{flow_name}")
            return flow
        
        except KeyError as e:
            self.logger.error(f"Configuration error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error getting flow config: {e}")
            raise
