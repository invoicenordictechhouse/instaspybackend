from fastapi import FastAPI
from auth.routes import auth_router
import uvicorn
import os

# Initialize FastAPI app
app = FastAPI()

# Include the auth_router (equivalent to registering Flask's blueprint)
app.include_router(auth_router, prefix="/api/auth")


# FastAPI automatically includes Swagger at /docs, so no manual Swagger setup is required
@app.get("/")
async def home():
    return {"message": "Welcome to the Authentication System"}


# Run the app if the script is called directly
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=8080)
