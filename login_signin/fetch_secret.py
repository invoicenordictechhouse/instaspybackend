from google.cloud import secretmanager

def access_secret_version(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Access the secret version from Google Cloud Secret Manager.
    
    Args:
        project_id (str): The Google Cloud project ID.
        secret_id (str): The ID of the secret (name of the secret you stored in Secret Manager).
        version_id (str): The version of the secret to retrieve (default is "latest").
    
    Returns:
        str: The secret payload (e.g., your JWT secret key).
    """
    client = secretmanager.SecretManagerServiceClient()

    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    response = client.access_secret_version(name=name)

    return response.payload.data.decode("UTF-8")
