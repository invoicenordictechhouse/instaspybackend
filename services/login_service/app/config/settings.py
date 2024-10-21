import os


class Config:
    """
    Config class to store configuration variables like token expiration and database information.
    These can be set as environment variables or hardcoded for local development.
    """

    # DB variables
    PROJECT_ID = "annular-net-436607-t0"
    DATASET_ID = "Instaspy_DS"
    TABLE_ID = "users"

    DEBUG = True

    # keycloak
    KEYCLOAK_SERVER_URL = "https://35.190.204.129:8443/"
    KEYCLOAK_REALM = "login_signup"
    KEYCLOAK_CLIENT_ID = "login-signup-backend-vm-gcp"
    KEYCLOAK_CLIENT_SECRET = "9TfagUllsUacNb9XTnYk2jBNf5AASXSA"
    KEYCLOAK_ADMIN_USER = "admin"
    KEYCLOAK_ADMIN_PASSWORD = "admin"

    # certs
    KEYCLOAK_CERT_PATH = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../server.crt")
    )
