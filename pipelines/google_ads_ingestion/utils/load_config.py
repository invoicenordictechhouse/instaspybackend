import os
import yaml


def load_config():
    """Loads the YAML configuration file."""
    config_file = os.getenv("CONFIG_FILE", "config/config.yml")
    with open(config_file, "r") as file:
        return yaml.safe_load(file)
