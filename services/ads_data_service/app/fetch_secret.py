# secrets_manager.py
import os
from google.cloud import secretmanager
import logging

logger = logging.getLogger(__name__)


def get_secret(secret_id: str, project_id: str = "your_project_id") -> str:
    """
    Fetches the latest version of a secret from Google Secret Manager.

    Args:
        secret_id (str): The ID of the secret in Google Secret Manager.
        project_id (str): Your GCP project ID.

    Returns:
        str: The secret value.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    try:
        response = client.access_secret_version(name=name)
        secret_value = response.payload.data.decode("UTF-8")
        logger.info(f"Successfully retrieved secret: {secret_id}")
        return secret_value
    except Exception as e:
        logger.error(f"Failed to access secret {secret_id}: {e}")
        raise
