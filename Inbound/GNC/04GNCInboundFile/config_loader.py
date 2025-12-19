# config_loader.py
import yaml
import os

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.yaml")

def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# For testing
if __name__ == "__main__":
    print(load_config())
