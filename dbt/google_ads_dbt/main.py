from routers import health, clean
import uvicorn
from fastapi import FastAPI

app = FastAPI()

app.include_router(health.router, prefix="/health", tags=["Health Check"])
app.include_router(clean.router, prefix="/clean", tags=["Cleaning"])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
