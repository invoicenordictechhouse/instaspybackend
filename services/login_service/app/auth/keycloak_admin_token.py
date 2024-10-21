import requests
from config.settings import Config
import os

cert_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../server.crt'))

def get_admin_token():
    url = "https://35.190.204.129:8443/realms/login_signup/protocol/openid-connect/token"

    data = {
        "client_id": "login-signup-backend-vm",
        "client_secret": "olhdkK25NInloZ4IE47D0z7mlabHqZ4A",
        "grant_type": "client_credentials",
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        # Send POST request to Keycloak to get admin token with correct headers
        response = requests.post(url, data=data, headers=headers, verify=False)
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
