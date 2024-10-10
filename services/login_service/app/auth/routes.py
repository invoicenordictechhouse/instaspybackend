from fastapi import FastAPI, APIRouter, HTTPException
from pydantic import BaseModel
from werkzeug.security import generate_password_hash, check_password_hash
from auth.jwt_utils import create_access_token
from store_fetch_user_db.fetch_user_from_db import get_user_from_bigquery
from store_fetch_user_db.store_user_in_db import insert_user_into_bigquery
from auth.valid_signup import is_valid_email, is_valid_password

app = FastAPI()

auth_router = APIRouter()


class SignupData(BaseModel):
    email: str
    password: str


class LoginData(BaseModel):
    email: str
    password: str


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

    if not insert_user_into_bigquery(email, hashed_password):
        raise HTTPException(status_code=500, detail="Failed to create user")

    return {"message": f"User {email} created successfully. You can now log in."}


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


app.include_router(auth_router, prefix="/api/auth")


@app.get("/")
async def root():
    return {"message": "Welcome to the Authentication System"}
