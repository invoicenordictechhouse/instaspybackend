class Config:
    """
    Config class to store configuration variables like token expiration and database information.
    These can be set as environment variables or hardcoded for local development.
    """

    # Token expiration time (in seconds) - e.g., 24 hours
    JWT_ACCESS_TOKEN_EXPIRES = 24 * 3600

    # DB variables
    PROJECT_ID = "annular-net-436607-t0"
    DATASET_ID = "Instaspy_DS"
    TABLE_ID = "users"

    DEBUG = True

    # keycloak
    KEYCLOAK_SERVER_URL = "http://localhost:8080"
    KEYCLOAK_REALM = "login_signup"
    KEYCLOAK_CLIENT_ID = "login-signup-backend-local"
    KEYCLOAK_CLIENT_SECRET = "T0gmgZNikVobBoEH7A8UocEmOmcey66h"
    KEYCLOAK_ADMIN_USER = "admin"
    KEYCLOAK_ADMIN_PASSWORD = "admin"
