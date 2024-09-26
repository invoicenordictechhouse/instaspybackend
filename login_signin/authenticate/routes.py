from flask import Blueprint, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from authenticate.jwt_utils import encode_jwt
from authenticate.jwt_middleware import jwt_required
from store_fetch_user_db.fetch_user_from_db import get_user_from_bigquery
from store_fetch_user_db.store_user_in_db import insert_user_into_bigquery
from authenticate.valid_signup import is_valid_email, is_valid_password

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/protected", methods=["GET"])
@jwt_required
def protected_route(user_email: str):
    """
    A protected route that only authenticated users can access.
    Args:
        user_email (str): The email of the authenticated user.
    Returns:
        A JSON response containing a message and the user's email.
    """

    return (
        jsonify(
            {
                "message": "You have accessed a protected route!",
                "user_email": user_email,
            }
        ),
        200,
    )


# Sign-Up Route
@auth_bp.route("/signup", methods=["POST"])
def signup():
    data = request.get_json()
    email = data.get("email")
    password = data.get("password")

    if not is_valid_email(email):
        return jsonify({"message": "Invalid email format"}), 400

    if not is_valid_password(password):
        return (
            jsonify(
                {
                    "message": "Password must be at least 8 characters long and contain letters and atleast one number"
                }
            ),
            400,
        )

    if get_user_from_bigquery(email):
        return jsonify({"message": "User already exists"}), 400

    hashed_password = generate_password_hash(password)
    if not insert_user_into_bigquery(email, hashed_password):
        return jsonify({"message": "Failed to create user"}), 500

    token = encode_jwt(email)  # Generate JWT token

    return (
        jsonify(
            {
                "message": f"User {email} created successfully",
                "access_token": token,  # Return JWT token for automatic login
            }
        ),
        201,
    )


@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    email = data.get("email")
    password = data.get("password")

    user = get_user_from_bigquery(email)
    if not user:
        return jsonify({"message": "User not found"}), 404

    # Verify password
    if not check_password_hash(user["password"], password):
        return jsonify({"message": "Invalid credentials"}), 401

    token = encode_jwt(email)
    return jsonify({"access_token": token, "message": "Login successful"}), 200
