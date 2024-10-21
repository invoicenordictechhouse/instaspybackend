import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException
from utils import bigquery_client, create_incremental_table_if_not_exists, fetch_google_ads_data
from schemas.backfill_request import BackfillRequest

router = APIRouter()

project_id = "annular-net-436607-t0"
dataset_id = "sample_ds"
table_id = "raw_sample_ads"
staging_table_id = "staging_active_sample_ids"
clean_table_id = "clean_google_ads"

@router.post(
    "/daily",
    summary="Run Daily Ingestion",
    description="Triggers the daily ingestion of Google Ads data.",
)
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
            raise HTTPException(
                status_code=500, detail="Failed to create or verify table"
            )

    except Exception as e:
        logging.error(f"Failed to initiate daily ingestion: {e}")
        raise HTTPException(status_code=500, detail="Daily ingestion failed")



@router.post(
    "/backfill",
    summary="Run Backfill Ingestion",
    description="Triggers a backfill ingestion for a specified date range.",
)
async def run_backfill(
    backfill_request: BackfillRequest, background_tasks: BackgroundTasks
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
            raise HTTPException(
                status_code=500, detail="Failed to create or verify table"
            )

    except Exception as e:
        logging.error(f"Failed to initiate backfill: {e}")
        raise HTTPException(status_code=500, detail="Backfill failed")

