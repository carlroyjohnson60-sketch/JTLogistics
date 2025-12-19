import os
import time
import json
import requests
from requests.auth import HTTPBasicAuth
from config_loader import load_config

config = load_config()
CACHE_FILE = "token_cache.json"

def get_access_token():
    # 1. Check cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
        if cache.get("expires_at", 0) > time.time():
            return cache["access_token"]

    # 2. Request new token
    auth_cfg = config["auth"]
    data = {
        "grant_type": "client_credentials",
        "scope": auth_cfg.get("scope", "")
    }

    response = requests.post(
        auth_cfg["token_url"],
        data=data,
        auth=HTTPBasicAuth(auth_cfg["client_id"], auth_cfg["client_secret"])
    )
    response.raise_for_status()

    token_data = response.json()
    access_token = token_data["access_token"]
    expires_in = token_data.get("expires_in", 3600)

    # 3. Cache token
    cache = {
        "access_token": access_token,
        "expires_at": time.time() + expires_in - 30
    }
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

    return access_token

if __name__ == "__main__":
    print("Access Token:", get_access_token())
