import os
import yaml


def load_config():
    """Loads the YAML configuration file."""
    config_file = os.getenv("CONFIG_FILE", "config/config.yml")
    with open(config_file, "r") as file:
        return yaml.safe_load(file)


ENV = os.getenv("ENV", "dev")


config_data = load_config()["environments"]

PROJECT_ID = config_data["project_id"]
DATASET_ID = config_data[ENV]["dataset_id"]
RAW_TABLE_ID = config_data[ENV]["raw_table_id"]
ADVERTISERS_TRACKING_TABLE_ID = config_data[ENV]["advertisers_tracking"]
LOG_LEVEL = config_data[ENV]["logging"]["log_level"]
