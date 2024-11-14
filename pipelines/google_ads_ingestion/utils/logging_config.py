import logging
from config import LOG_LEVEL

logging.basicConfig(
    # Converts "DEBUG" or "ERROR" to logging.DEBUG or logging.ERROR
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger("GoogleAdsIngestion")
