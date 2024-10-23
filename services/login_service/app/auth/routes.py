from fastapi import FastAPI, APIRouter, Depends, HTTPException
from pydantic import BaseModel
from auth.valid_signup import is_valid_email, is_valid_password
from email_verification.email_services import send_verification_email
from verify_table.store_verification_code import store_verification_code
from queries import GET_VERIFICATION_CODE
from fastapi.security import OAuth2AuthorizationCodeBearer
from config.settings import Config
from auth.keycloak_utils import (
    get_token,
    verify_token,
    create_keycloak_user,
    activate_keycloak_user,
    check_user_exists_in_keycloak,
)
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

app = FastAPI()

auth_router = APIRouter()


oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl=f"{Config.KEYCLOAK_SERVER_URL}/realms/{Config.KEYCLOAK_REALM}/protocol/openid-connect/auth",
    tokenUrl=f"{Config.KEYCLOAK_SERVER_URL}/realms/{Config.KEYCLOAK_REALM}/protocol/openid-connect/token",
)


class SignupData(BaseModel):
    email: str
    password: str


class LoginData(BaseModel):
    email: str
    password: str


class VerifyEmailData(BaseModel):
    email: str
    verification_code: str


@auth_router.get("/protected")
async def protected_route(token: str = Depends(verify_token)):
    return {"message": "You have accessed a protected route!", "user_info": token}


@auth_router.post("/signup")
async def signup(data: SignupData):
    email = data.email
    password = data.password

    if not is_valid_email(email):
        raise HTTPException(status_code=400, detail="Invalid email format")

    if not is_valid_password(password):
        raise HTTPException(
            status_code=400,
            detail="Password must be at least 8 characters long and contain letters and at least one number",
        )

    if check_user_exists_in_keycloak(email):
        raise HTTPException(status_code=400, detail="User already exists")

    if not create_keycloak_user(email, password):
        raise HTTPException(status_code=500, detail="Failed to create user in Keycloak")

    email_sent, code = send_verification_email(email)
    store_verification_code(email=email, verification_code=code)

    if not email_sent:
        raise HTTPException(status_code=500, detail="Failed to send verification email")

    return {"message": f"User {email} created successfully. Please verify your email."}


@auth_router.post("/login")
async def login(data: LoginData):
    email = data.email
    password = data.password

    if not check_user_exists_in_keycloak(email):
        raise HTTPException(
            status_code=400, detail="User does not exists or invalid credentials"
        )

    try:
        token = get_token(email, password)
        if not token:
            raise HTTPException(
                status_code=500, detail="Failed to feth token from Keycloak"
            )

        return {"access_token": token, "message": "Login successful"}
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid credentials or Keycloak error {e}"
        )


@auth_router.post("/verify_email")
async def verify_email(data: VerifyEmailData):
    email = data.email
    verification_code = data.verification_code

    if not email or not verification_code:
        raise HTTPException(
            status_code=400, detail="Email and verification code are required"
        )

    client = bigquery.Client()

    query = GET_VERIFICATION_CODE

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email)]
    )

    query_job = client.query(query, job_config=job_config)
    result = query_job.result()

    row = None
    for row in result:
        break

    if not row:
        raise HTTPException(status_code=400, detail="Unknown email. ")

    stored_code = row.verification_code
    created_at = row.created_at
    current_time = datetime.now(timezone.utc)

    if stored_code != verification_code:
        raise HTTPException(status_code=400, detail="Invalid verification code")

    if current_time > created_at + timedelta(minutes=10):
        raise HTTPException(status_code=400, detail="Verification code has expired")

    if not activate_keycloak_user(email):
        raise HTTPException(
            status_code=500, detail="Failed to activate user in Keycloak"
        )

    # delete_verification_code(email)
    return {"message": "Email verified successfully!"}


app.include_router(auth_router, prefix="/api/auth")


@app.get("/")
async def root():
    return {"message": "Welcome to the Authentication System"}
