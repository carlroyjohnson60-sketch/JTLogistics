"""OAuth2 client for token management with file-based caching.

Usage:
    oauth = OAuthClient(cfg.get_auth())  # Get auth config section
    token = oauth.get_token()            # Get valid token (from cache or new)
    headers = {'Authorization': f'Bearer {token}'}
"""
import os
import time
import json
import logging
try:
    import requests
except Exception:
    import urllib.request as _urllib_request
    import urllib.parse as _urllib_parse
    import urllib.error as _urllib_error
    import base64 as _base64

    class _Response:
        def __init__(self, status, data):
            self.status_code = status
            self._data = data

        def raise_for_status(self):
            if not (200 <= self.status_code < 300):
                raise Exception(f"HTTP {self.status_code}: {self._data}")

        def json(self):
            return json.loads(self._data)

    class requests:
        @staticmethod
        def post(url, data=None, auth=None):
            encoded = _urllib_parse.urlencode(data or {}).encode('utf-8')
            req = _urllib_request.Request(url, data=encoded)
            if auth:
                creds = f"{auth[0]}:{auth[1]}".encode('utf-8')
                b64 = _base64.b64encode(creds).decode('ascii')
                req.add_header('Authorization', f'Basic {b64}')
            req.add_header('Content-Type', 'application/x-www-form-urlencoded')
            try:
                with _urllib_request.urlopen(req) as resp:
                    body = resp.read().decode('utf-8')
                    status = resp.getcode()
            except _urllib_error.HTTPError as e:
                try:
                    body = e.read().decode('utf-8')
                except Exception:
                    body = ''
                status = e.code
            return _Response(status, body)

from typing import Optional, Dict


class OAuthClient:
    def __init__(self, cfg: dict):
        """Initialize with auth config section from config.yaml."""
        self.token_url = cfg.get('token_url')
        self.client_id = cfg.get('client_id')
        self.client_secret = cfg.get('client_secret')
        self.scope = cfg.get('scope', '')
        self.cache_file = cfg.get('cache_file', 'token_cache.json')
        self._cache: Optional[Dict] = None
        self.logger = logging.getLogger(__name__)

    def _load_cache(self) -> Dict:
        """Load token cache from file with error handling."""
        if self._cache is None:
            try:
                if os.path.exists(self.cache_file):
                    with open(self.cache_file, 'r') as f:
                        self._cache = json.load(f)
                    self.logger.debug(f"Loaded token cache from {self.cache_file}")
                else:
                    self._cache = {}
            except json.JSONDecodeError as e:
                self.logger.warning(f"Invalid JSON in token cache: {e}")
                self._cache = {}
            except IOError as e:
                self.logger.warning(f"Error reading token cache: {e}")
                self._cache = {}
            except Exception as e:
                self.logger.error(f"Unexpected error loading cache: {e}")
                self._cache = {}
        return self._cache

    def _save_cache(self, token_data: Dict):
        """Save token data to cache file with error handling."""
        self._cache = token_data
        try:
            cache_dir = os.path.dirname(self.cache_file)
            if cache_dir:
                os.makedirs(cache_dir, exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(token_data, f)
            self.logger.debug(f"Saved token cache to {self.cache_file}")
        except IOError as e:
            self.logger.warning(f"Failed to save token cache: {e}")
        except Exception as e:
            self.logger.warning(f"Error saving cache: {e}")

    def get_token(self) -> str:
        """Get a valid access token, from cache if possible or request new one.
        
        Returns:
            str: OAuth2 access token
            
        Raises:
            Exception: If token retrieval fails
        """
        try:
            cache = self._load_cache()
            
            # Check if cached token is still valid
            if cache and cache.get('expires_at', 0) > time.time():
                self.logger.debug("Using cached token")
                return cache['access_token']

            # Request new token
            self.logger.info(f"Requesting new token from {self.token_url}")
            data = {
                'grant_type': 'client_credentials',
                'scope': self.scope
            }

            try:
                response = requests.post(
                    self.token_url,
                    data=data,
                    auth=(self.client_id, self.client_secret)
                )
                response.raise_for_status()
            except Exception as e:
                self.logger.error(f"Token request failed: {e}", exc_info=True)
                raise

            try:
                token_data = response.json()
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse token response: {e}")
                raise
            
            access_token = token_data.get('access_token')
            if not access_token:
                self.logger.error(f"No access_token in response: {token_data}")
                raise ValueError("No access_token in OAuth response")
            
            expires_in = token_data.get('expires_in', 3600)

            # Cache token (expires 30s early for safety)
            cache = {
                'access_token': access_token,
                'expires_at': time.time() + expires_in - 30
            }
            self._save_cache(cache)
            self.logger.info("Token successfully obtained and cached")

            return access_token
        
        except Exception as e:
            self.logger.error(f"Error getting token: {e}", exc_info=True)
            raise

    def get_auth_headers(self) -> Dict[str, str]:
        """Get Authorization headers with valid token.
        
        Returns:
            dict: Authorization header dict
        """
        try:
            token = self.get_token()
            return {'Authorization': f'Bearer {token}'}
        except Exception as e:
            self.logger.error(f"Error getting auth headers: {e}")
            return {}