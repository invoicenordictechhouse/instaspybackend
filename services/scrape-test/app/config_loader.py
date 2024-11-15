# config_loader.py

import yaml
import os
from typing import Dict, Any


def load_config() -> Dict[str, Any]:
    """
    Load the configuration from a YAML file.

    Returns:
        Dict[str, Any]: The configuration dictionary.
    """
    config_file = os.getenv("CONFIG_FILE", "app/config.yaml")
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config


config: Dict[str, Any] = load_config()
"""
Global configuration dictionary loaded from `config.yaml`.
"""
