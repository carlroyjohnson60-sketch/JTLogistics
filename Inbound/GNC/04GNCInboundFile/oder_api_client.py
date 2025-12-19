import os
import time
import json
import requests
from requests.auth import HTTPBasicAuth
from config_loader import load_config
from get_token import get_access_token

# Load configuration once
config = load_config()


def call_order_api(order_data):
    """
    Call order API with the latest access token and JSON data.

    Parameters
    ----------
    order_data : dict | str
        The order data as a Python dictionary (preferred),
        or a file path to a JSON file (for backward compatibility).

    Returns
    -------
    dict
        Parsed API response (JSON) or an error dict.
    """
    # If a file path (string) is passed, load JSON from it
    if isinstance(order_data, (str, os.PathLike)):
        with open(order_data, "r", encoding="utf-8") as f:
            order_data = json.load(f)

    # Ensure we have a dictionary
    if not isinstance(order_data, dict):
        raise TypeError("order_data must be a dict or path to a JSON file")

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            config["api"]["asn_url"],
            headers=headers,
            json=order_data,
            timeout=30
        )

        print("📡 Status:", response.status_code)
        try:
            resp_json = response.json()
            print("✅ Response:", json.dumps(resp_json, indent=2))
            return resp_json
        except Exception:
            print("⚠️ Raw Response:", response.text)
            return {"error": response.text, "status": response.status_code}

    except requests.exceptions.RequestException as e:
        print("❌ Request failed:", e)
        return {"error": str(e), "status": "request_failed"}


if __name__ == "__main__":
    # Example usage
    test_data = {"orders": [{"order_id": "TEST123", "status": "Pending"}]}
    call_order_api(test_data)
