import os

class Config:
    """
    Config class to store configuration variables like JWT secret key and token expiration.
    These can be set as environment variables or hardcoded for local development.
    """
    # Token expiration time (in seconds) - e.g., 24 hours
    JWT_ACCESS_TOKEN_EXPIRES = 24 * 3600

    PROJECT_ID = "annular-net-436607-t0"
    DATASET_ID = "Instaspy_DS"
    TABLE_ID = "users"