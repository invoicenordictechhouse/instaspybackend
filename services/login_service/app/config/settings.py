from fetch_secret import access_secret_version
from certificates import save_certificate_as_temp_file


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
    KEYCLOAK_CLIENT_ID = access_secret_version(PROJECT_ID, "Keycloak_client_id")
    KEYCLOAK_CLIENT_SECRET = access_secret_version(PROJECT_ID, "Keycloak_client_secret")
    KEYCLOAK_ADMIN_USER = "admin"
    KEYCLOAK_ADMIN_PASSWORD = access_secret_version(
        PROJECT_ID, "Keycloak_admin_password"
    )

    # certs

    KEYCLOAK_CERT_PATH = save_certificate_as_temp_file(
        access_secret_version(PROJECT_ID, "server_cert")
    )
