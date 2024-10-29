from fastapi import APIRouter, BackgroundTasks, HTTPException
from schemas.BackfillRequest import BackfillRequest
from services.ingestion_service import run_daily_ingestion, run_backfill_ingestion

router = APIRouter()


@router.post(
    "/daily",
    summary="Run Daily Ingestion",
    description="Triggers the daily ingestion of Google Ads data.",
)
async def daily_ingestion(background_tasks: BackgroundTasks):
    """
    This endpoint triggers the ingestion of Google Ads data for the previous day.

    It checks if the necessary BigQuery table exists and then initiates the data fetch.
    The task runs in the background.

    Raises:
        HTTPException: If the ingestion fails or if the table cannot be created or verified.

    """
    try:
        background_tasks.add_task(run_daily_ingestion)
        return {"status": "Daily ingestion initiated"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Daily ingestion failed: {str(e)}")


@router.post(
    "/backfill",
    summary="Run Backfill Ingestion",
    description="Triggers a backfill ingestion for a specified date range.",
)
async def backfill_ingestion(
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
        background_tasks.add_task(
            run_backfill_ingestion,
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

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Backfill ingestion failed: {str(e)}"
        )
