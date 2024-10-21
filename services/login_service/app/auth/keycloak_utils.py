from keycloak import KeycloakOpenID
from config.settings import Config
import requests
from auth.keycloak_admin_token import get_admin_token
from fastapi import HTTPException


keycloak_openid = KeycloakOpenID(
    server_url=Config.KEYCLOAK_SERVER_URL,
    client_id=Config.KEYCLOAK_CLIENT_ID,
    realm_name=Config.KEYCLOAK_REALM,
    client_secret_key=Config.KEYCLOAK_CLIENT_SECRET,
    verify=Config.KEYCLOAK_CERT_PATH,
)


def get_token(username, password):
    try:
        token = keycloak_openid.token(username, password)
        return token
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to get token: {str(e)}")


def get_user_info(token):
    return keycloak_openid.userinfo(token)


def introspect_token(token):
    return keycloak_openid.introspect(token)


def verify_token(token):
    introspection = keycloak_openid.introspect(token)
    if introspection.get("active"):
        return introspection
    else:
        raise Exception("Invalid or expired token")


def create_keycloak_user(email, password):
    # Endpoint to create users
    url = f"{Config.KEYCLOAK_SERVER_URL}admin/realms/{Config.KEYCLOAK_REALM}/users"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_admin_token()}",
    }

    payload = {
        "enabled": False,
        "username": email,
        "email": email,
        "credentials": [{"type": "password", "value": password, "temporary": False}],
        "emailVerified": False,
    }

    response = requests.post(
        url, json=payload, headers=headers, verify=Config.KEYCLOAK_CERT_PATH
    )

    if response.status_code == 201:
        return True  # User successfully created
    else:
        return False  # Handle the error


def activate_keycloak_user(email):
    url = f"{Config.KEYCLOAK_SERVER_URL}admin/realms/{Config.KEYCLOAK_REALM}/users?email={email}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_admin_token()}",
    }

    response = requests.get(url, headers=headers, verify=Config.KEYCLOAK_CERT_PATH)
    if response.status_code == 200 and response.json():
        user_id = response.json()[0]["id"]
        activation_url = f"{Config.KEYCLOAK_SERVER_URL}/admin/realms/{Config.KEYCLOAK_REALM}/users/{user_id}"
        payload = {"enabled": True, "emailVerified": True}  # Activating the user

        activate_response = requests.put(
            activation_url,
            json=payload,
            headers=headers,
            verify=Config.KEYCLOAK_CERT_PATH,
        )

        if activate_response.status_code == 204:
            return True  # User activated successfully
    return False  # Handle activation failure


def check_user_exists_in_keycloak(email):
    token = get_admin_token()

    url = f"{Config.KEYCLOAK_SERVER_URL}/admin/realms/{Config.KEYCLOAK_REALM}/users?email={email}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers, verify=Config.KEYCLOAK_CERT_PATH)

    if response.status_code == 200:
        users = response.json()
        if users:  # If the list is not empty, the user exists
            return True
    return False
