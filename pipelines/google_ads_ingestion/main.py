import logging
from utils.request_schemas import BackfillRequest, UpdateRequest
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from utils.updateEnum import UpdateMode
from utils import (
    bigquery_client,
    fetch_google_ads_data,
    create_incremental_table_if_not_exists,
    update_existing_google_ads_data,
)

app = FastAPI()
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting Google Ads Data Ingestion API...")
    yield
    print("Shutting down Google Ads Data Ingestion API...")


app = FastAPI(lifespan=lifespan)

project_id = "annular-net-436607-t0"
dataset_id = "sample_ds"
table_id = "raw_sample_ads"
staging_table_id = "staging_active_sample_ids"
clean_table_id = "clean_google_ads"

@app.get("/", summary="Health Check", description="Root endpoint for health check.")
async def root():
    """
    Root endpoint that provides an overview of the API and performs a basic health check. 
    It lists the available endpoints along with their purpose and expected parameters.
    """
    return {
        "message": "Google Ads Data Ingestion API is running",
        "endpoints": {
            "/daily": {
                "summary": "Daily Ingestion",
                "description": "Triggers the ingestion of Google Ads data for the previous day.",
                "method": "POST",
                "parameters": None,
                "response": {
                    "status": "Daily ingestion initiated"
                }
            },
            "/backfill": {
                "summary": "Backfill Ingestion",
                "description": "Triggers backfill ingestion of Google Ads data for a specific date range.",
                "method": "POST",
                "parameters": {
                    "start_date": {
                        "description": "The start date for the backfill in 'YYYY-MM-DD' format.",
                        "required": True,
                        "example": "2023-01-01"
                    },
                    "end_date": {
                        "description": "The end date for the backfill in 'YYYY-MM-DD' format.",
                        "required": True,
                        "example": "2023-01-31"
                    },
                    "advertiser_ids": {
                        "description": "A list of advertiser IDs to backfill data for.",
                        "required": True,
                        "example": ["AD12345", "AD67890"]
                    }
                },
                "response": {
                    "status": "Backfill initiated",
                    "start_date": "YYYY-MM-DD",
                    "end_date": "YYYY-MM-DD",
                    "advertiser_ids": ["AD12345", "AD67890"]
                }
            },
            "/update": {
                "summary": "Update Ads Data",
                "description": "Triggers the update of Google Ads data based on the selected mode.",
                "method": "POST",
                "parameters": {
                    "update_mode": {
                        "description": "Defines the update mode for the operation.",
                        "type": "enum",
                        "values": {
                            "ALL": "Update all ads",
                            "ACTIVE": "Update only active ads",
                            "SPECIFIC": "Update specific ads based on advertiser or creative IDs"
                        },
                        "required": True,
                        "example": "SPECIFIC"
                    },
                    "advertiser_ids": {
                        "description": "A list of advertiser IDs for updating specific ads. Required if 'update_mode' is 'SPECIFIC'.",
                        "required": False,
                        "example": ["AD12345", "AD67890"]
                    },
                    "creative_ids": {
                        "description": "A list of creative IDs for updating specific ads. Required if 'update_mode' is 'SPECIFIC'.",
                        "required": False,
                        "example": ["CR012345", "CR678910"]
                    }
                },
                "response": {
                    "status": "Update initiated",
                    "update_mode": "SPECIFIC"
                }
            }
        }
    }



@app.post("/daily/", summary="Run Daily Ingestion", description="Triggers the daily ingestion of Google Ads data.")
async def run_daily(background_tasks: BackgroundTasks):
    """
    This endpoint triggers the ingestion of Google Ads data for the previous day.
    
    It checks if the necessary BigQuery table exists and then initiates the data fetch.
    The task runs in the background.
    
    Raises:
        HTTPException: If the ingestion fails or if the table cannot be created or verified.

    """
    try:
        table_exists = create_incremental_table_if_not_exists(
            bigquery_client, dataset_id, table_id
        )

        if table_exists:
            background_tasks.add_task(
                fetch_google_ads_data, dataset_id, project_id, table_id
            )
            return {"status": "Daily ingestion initiated"}
        else:
            raise HTTPException(status_code=500, detail="Failed to create or verify table")

    except Exception as e:
        logging.error(f"Failed to initiate daily ingestion: {e}")
        raise HTTPException(status_code=500, detail="Daily ingestion failed")


@app.post("/backfill/", summary="Run Backfill Ingestion", description="Triggers a backfill ingestion for a specified date range.")
async def run_backfill(
    backfill_request: BackfillRequest,background_tasks: BackgroundTasks
):
    """
    This endpoint triggers a backfill ingestion of Google Ads data for a specified date range.

    Parameters:
        backfill_request (BackfillRequest): Contains the start date, end date, and advertiser IDs.

    Raises:
        HTTPException: If the ingestion fails or if the table cannot be created or verified.

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
                backfill_request.start_date,
                backfill_request.end_date,
                backfill_request.advertiser_ids,
            )
            return {
                "status": "Backfill initiated",
                "start_date": backfill_request.start_date,
                "end_date": backfill_request.end_date,
                "advertiser_ids": backfill_request.advertiser_ids,

            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create or verify table")


    except Exception as e:
        logging.error(f"Failed to initiate backfill: {e}")
        raise HTTPException(status_code=500, detail="Backfill failed")

@app.post("/update/", summary="Run Update", description="Triggers an update of Google Ads data based on the mode (ALL, ACTIVE, SPECIFIC).")
async def run_update(update_request: UpdateRequest, background_tasks: BackgroundTasks):
    """
    This endpoint triggers an update of Google Ads data.

    The mode can be one of the following:
    - 0 (ALL): Update all ads in the dataset.
    - 1 (ACTIVE): Update only active ads.
    - 2 (SPECIFIC): Update specific ads (requires either advertiser_ids or creative_ids).

    Parameters:
        update_request (UpdateRequest): Contains the update mode and optional advertiser IDs or creative IDs.

    Raises:
        HTTPException: If no advertiser or creative IDs are provided for the SPECIFIC mode or if the update fails.
    
    """
    if update_request.update_mode == UpdateMode.SPECIFIC:
        if not update_request.advertiser_ids and not update_request.creative_ids:
            raise HTTPException(
                    status_code=400, 
                    detail="For SPECIFIC mode, either 'advertiser_ids' or 'creative_ids' must be provided."
            )

    try:
        background_tasks.add_task(
            update_existing_google_ads_data,
            update_request.update_mode,
            project_id,
            dataset_id,
            table_id,
            staging_table_id,
            update_request.advertiser_ids,
            update_request.creative_ids 
        )

        return {
            "status": "Update initiated", 
            "update_mode": update_request.update_mode,
            "description": f"Mode: {update_request.update_mode} - {'ALL' if update_request.update_mode == 0 else 'ACTIVE' if update_request.update_mode == 1 else 'SPECIFIC'}"
        }

    except Exception as e:
        logging.error(f"Failed to initiate daily ingestion: {e}")
        raise HTTPException(status_code=500, detail="Daily ingestion failed")



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
