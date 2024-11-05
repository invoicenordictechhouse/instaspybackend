# logging_config.py

import logging
from config_loader import config

logging.basicConfig(
    level=config["logging"]["log_level"],
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger("YouTubeScraper")
"""
Logger instance for the application.
"""
