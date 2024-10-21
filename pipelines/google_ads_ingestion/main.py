import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from routers import ads, health, ingestion

project_id = "annular-net-436607-t0"
dataset_id = "sample_ds"
table_id = "raw_sample_ads"
staging_table_id = "staging_active_sample_ids"
clean_table_id = "clean_google_ads"

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting Google Ads Data Ingestion API...")
    yield
    print("Shutting down Google Ads Data Ingestion API...")

app = FastAPI(lifespan=lifespan)

app.include_router(ads.router, prefix="/ingestion/ads", tags=["Ads Update"])
app.include_router(ingestion.router, prefix="/ingestion", tags=["Ingestion"])
app.include_router(health.router, prefix="/health", tags=["Health"])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
