import jwt
from flask import jsonify
from datetime import datetime, timedelta, timezone
from config.settings import Config
from fetch_secret import access_secret_version
import logging


# Encode JWT Token
def encode_jwt(user_email: str) -> str:
    """
    Encodes a JWT token with the user's email and expiration time.

    Args:
        user_email (str): The email of the user for which the token is generated.

    Returns:
        str: A JWT token.
    """
    payload = {
        "email": user_email,
        "exp": datetime.now(timezone.utc)
        + timedelta(seconds=Config.JWT_ACCESS_TOKEN_EXPIRES),
    }
    jwt_secret_key = access_secret_version(Config.PROJECT_ID, "jwt-secret")
    token = jwt.encode(payload, jwt_secret_key, algorithm="HS256")
    return token


# Decode JWT Token
def decode_jwt(token: str) -> str:
    """
    Decodes the JWT token and returns the user's email.

    Args:
        token (str): The JWT token to decode.

    Returns:
        str: The user's email from the decoded token, or None if invalid.
    """
    try:
        jwt_secret_key = access_secret_version(Config.PROJECT_ID, "jwt-secret")
        payload = jwt.decode(token, jwt_secret_key, algorithms=["HS256"])
        return payload["email"]
    except jwt.ExpiredSignatureError:
        logging.warning("Token has expired")
        return jsonify({"message": "Session expired, please log in again"}), 401

    except jwt.InvalidTokenError:
        logging.warning("Invalid token")
        return jsonify({"message": "Invalid authentication token"}), 401
