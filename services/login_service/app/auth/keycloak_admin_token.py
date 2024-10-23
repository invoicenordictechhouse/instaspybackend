import requests
from config.settings import Config


def get_admin_token() -> str:
    url = (
        f"{Config.KEYCLOAK_SERVER_URL}realms/login_signup/protocol/openid-connect/token"
    )

    data = {
        "client_id": Config.KEYCLOAK_CLIENT_ID,
        "client_secret": Config.KEYCLOAK_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        # Send POST request to Keycloak to get admin token with correct headers
        response = requests.post(
            url, data=data, headers=headers, verify=Config.KEYCLOAK_CERT_PATH
        )
        response.raise_for_status()  # Raise an error for 4xx/5xx responses

        token_data = response.json()
        if "access_token" in token_data:
            return token_data["access_token"]
        else:
            raise Exception("No access token in response")

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        raise Exception("Failed to connect to Keycloak to get admin token")

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise Exception("Failed to get admin token")
