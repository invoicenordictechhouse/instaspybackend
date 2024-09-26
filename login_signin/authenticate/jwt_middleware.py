from flask import request, jsonify
from functools import wraps
from authenticate.jwt_utils import decode_jwt


def jwt_required(f):
    """
    A decorator that checks for a valid JWT token in the request header.

    Args:
        f: The wrapped route function that requires authentication.

    Returns:
        The original function if the JWT token is valid, or a 401 error if invalid.
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get("Authorization")

        if not token:
            return jsonify({"message": "Token is missing!"}), 401

        # Token comes in the format 'Bearer <token>', so we split it
        token = token.split()[1]

        user_email = decode_jwt(token)  # Decode the token and get the user email

        if not user_email:
            return jsonify({"message": "Token is invalid or expired!"}), 401

        # If the token is valid, allow the user to access the protected route
        return f(user_email=user_email, *args, **kwargs)

    return decorated_function
