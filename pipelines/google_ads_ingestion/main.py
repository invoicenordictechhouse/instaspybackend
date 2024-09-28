import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from utils import (
    bigquery_client,
    fetch_google_ads_data,
    create_incremental_table_if_not_exists,
)

app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting Google Ads Data Ingestion API...")
    yield
    print("Shutting down Google Ads Data Ingestion API...")


app = FastAPI(lifespan=lifespan)

project_id = "annular-net-436607-t0"
dataset_id = "instaspy_DBUS"
table_id = "raw_google_ads"


@app.get("/")
async def root():
    """
    Root endpoint for health check.
    """
    return {
        "message": "Google Ads Data Ingestion API is running",
        "available_endpoints": ["/daily", "/backfill"],
    }


@app.post("/daily/")
async def run_daily(background_tasks: BackgroundTasks):
    """
    Endpoint to trigger daily ingestion of Google Ads data (fetches the previous day's data).
    """
    try:
        table_exists = create_incremental_table_if_not_exists(
            bigquery_client, dataset_id, table_id
        )

        if table_exists:
            background_tasks.add_task(
                fetch_google_ads_data, dataset_id, project_id, table_id, False
            )
            return {"status": "Daily ingestion initiated"}
        else:
            return {"status": "Failed to create or verify table"}

    except Exception as e:
        logging.error(f"Failed to initiate daily ingestion: {e}")
        raise HTTPException(status_code=500, detail="Daily ingestion failed")


@app.get("/backfill/{start_date}/{end_date}")
async def run_backfill(
    start_date: str, end_date: str, background_tasks: BackgroundTasks
):
    """
    Endpoint to trigger backfill of Google Ads data for a specified date range.
    """
    try:
        table_exists = create_incremental_table_if_not_exists(
            bigquery_client, dataset_id, table_id
        )

        if table_exists:
            background_tasks.add_task(
                fetch_google_ads_data,
                dataset_id,
                project_id,
                table_id,
                True,
                start_date,
                end_date,
            )
            return {
                "status": "Backfill initiated",
                "start_date": start_date,
                "end_date": end_date,
            }
        else:
            return {"status": "Failed to create or verify table"}

    except Exception as e:
        logging.error(f"Failed to initiate backfill: {e}")
        raise HTTPException(status_code=500, detail="Backfill failed")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
