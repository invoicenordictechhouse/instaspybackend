from fastapi import FastAPI, APIRouter, Depends, HTTPException
from pydantic import BaseModel
from werkzeug.security import generate_password_hash, check_password_hash
from auth.jwt_utils import create_access_token
from auth.jwt_middleware import jwt_required
from store_fetch_user_db.fetch_user_from_db import get_user_from_bigquery
from store_fetch_user_db.store_user_in_db import insert_user_into_bigquery
from auth.valid_signup import is_valid_email, is_valid_password
from email_verification.email_services import send_verification_email
from verify_table.store_verification_code import store_verification_code
from queries import GET_VERIFICATION_CODE

# from verify_table.delet_row_verify import delete_verification_code
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

app = FastAPI()

auth_router = APIRouter()


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
async def protected_route(user_email: str = Depends(jwt_required)):
    return {"message": "You have accessed a protected route!", "user_email": user_email}


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

    if get_user_from_bigquery(email):
        raise HTTPException(status_code=400, detail="User already exists")

    hashed_password = generate_password_hash(password)

    email_sent, code = send_verification_email(email)
    store_verification_code(
        email=email, verification_code=code, hashed_password=hashed_password
    )

    if not email_sent:
        raise HTTPException(status_code=500, detail="Failed to send verification email")

    token = create_access_token({"sub": email})
    return {
        "message": f"User {email} created successfully. Please verify your email.",
        "access_token": token,
    }


@auth_router.post("/login")
async def login(data: LoginData):
    email = data.email
    password = data.password

    user = get_user_from_bigquery(email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if not check_password_hash(user["password"], password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({"sub": email})
    return {"access_token": token, "message": "Login successful"}


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
    hashed_password = row.hashed_password
    current_time = datetime.now(timezone.utc)

    if stored_code != verification_code:
        raise HTTPException(status_code=400, detail="Invalid verification code")

    if current_time > created_at + timedelta(minutes=10):
        raise HTTPException(status_code=400, detail="Verification code has expired")

    if not insert_user_into_bigquery(email, hashed_password):
        raise HTTPException(status_code=500, detail="Failed to create user")

    # delete_verification_code(email)
    return {"message": "Email verified successfully!"}


app.include_router(auth_router, prefix="/api/auth")


@app.get("/")
async def root():
    return {"message": "Welcome to the Authentication System"}
