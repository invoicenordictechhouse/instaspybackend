from routers import clean, health, consume_prep
import uvicorn
from fastapi import FastAPI

app = FastAPI()

app.include_router(health.router, tags=["Health Check"])
app.include_router(clean.router, prefix="/clean", tags=["Cleaning"])
app.include_router(consume_prep.router, prefix="/consume-prep", tags=["Consume-prep"])


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
