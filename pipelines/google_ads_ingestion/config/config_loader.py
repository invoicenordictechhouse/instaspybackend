import os
import yaml
from .models import Config


def load_config() -> Config:
    """Loads the YAML configuration file."""
    config_file = os.getenv("CONFIG_FILE", "config/config.yml")
    with open(config_file, "r") as file:
        config_data = yaml.safe_load(file)
        return Config(**config_data)


ENV = os.getenv("ENV", "dev")


config = load_config()
current_env_config = config.environments.get(ENV)

PROJECT_ID = config.project_id
DATASET_ID = current_env_config.dataset_id
RAW_TABLE_ID = current_env_config.raw_table_id
ADVERTISERS_TRACKING_TABLE_ID = current_env_config.advertisers_tracking
LOG_LEVEL = current_env_config.logging["log_level"]
