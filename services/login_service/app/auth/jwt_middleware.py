from fastapi import Request, HTTPException
from auth.jwt_utils import decode_access_token


async def jwt_required(request: Request):
    """
    A dependency function that checks for a valid JWT token in the request header.

    Args:
        request: The FastAPI request object containing the headers.

    Returns:
        The user_email if the JWT token is valid, otherwise raises HTTPException with 401 status.
    """
    token = request.headers.get("Authorization")

    if not token:
        raise HTTPException(status_code=401, detail="Token is missing!")

    try:
        token = token.split()[1]
    except IndexError:
        raise HTTPException(status_code=401, detail="Invalid token format!")

    decoded_token = decode_access_token(token)

    if not decoded_token:
        raise HTTPException(status_code=401, detail="Token is invalid or expired!")

    return decoded_token
