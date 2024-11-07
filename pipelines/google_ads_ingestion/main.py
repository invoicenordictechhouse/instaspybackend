import uvicorn
from fastapi import FastAPI
from routers import health, ads, ingestion

app = FastAPI()

app.include_router(health.router, tags=["Health"])
app.include_router(ads.router, prefix="/ads", tags=["Ads Update"])
app.include_router(ingestion.router, prefix="/ingestion", tags=["Ingestion"])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
