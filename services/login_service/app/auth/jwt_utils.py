import jwt
from fastapi import HTTPException
from datetime import datetime, timedelta, timezone
from config.settings import Config
from fetch_secret import access_secret_version

ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24


def create_access_token(data: dict):
    """
    Create a JWT token with an expiration time.
    Args:
        data (dict): The data to include in the token payload.
    Returns:
        str: The JWT token as a string.
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})  # Add expiration time to the token
    encoded_jwt = jwt.encode(
        to_encode,
        access_secret_version(Config.PROJECT_ID, "jwt-secret"),
        algorithm="HS256",
    )
    return encoded_jwt


def decode_access_token(token: str):
    """
    Decode and validate the JWT token.
    Args:
        token (str): The JWT token to decode.
    Returns:
        dict: The decoded token payload.
    Raises:
        HTTPException: If the token is invalid or expired.
    """
    try:
        decoded_token = jwt.decode(
            token,
            access_secret_version(Config.PROJECT_ID, "jwt-secret"),
            algorithm="HS256",
        )
        return decoded_token
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
