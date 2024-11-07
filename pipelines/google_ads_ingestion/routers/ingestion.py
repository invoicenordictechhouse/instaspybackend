from utils.logging_config import logger
from fastapi import APIRouter, HTTPException
from schemas.BackfillRequest import BackfillRequest
from services.ingestion_service import run_daily_ingestion, run_backfill_ingestion

router = APIRouter()


@router.post(
    "/daily",
    summary="Run Daily Ingestion",
    description="Triggers the daily ingestion of Google Ads data for the previous day, ensuring the latest data is inserted into BigQuery.",
)
async def daily_ingestion():
    """
    This endpoint triggers the ingestion of Google Ads data for the previous day.

    **Description**:
    - It checks if the necessary BigQuery table exists and creates it if necessary.
    - Initiates data ingestion for the previous day’s data.

    **Returns**:
    - A success message if new data is ingested.
    - A no-content message if data is already up-to-date.

    **Raises**:
    - HTTPException if ingestion fails or if table verification fails.
    """
    try:
        return run_daily_ingestion()

    except HTTPException as http_exc:
        raise http_exc
    except Exception:
        logger.error("Unexpected error during daily ingestion", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"status": "Unexpected error during daily ingestion"},
        )


@router.post(
    "/backfill",
    summary="Run Backfill Ingestion",
    description="Triggers a backfill ingestion for a specified date range.",
)
async def backfill_ingestion(backfill_request: BackfillRequest):
    """
    This endpoint triggers a backfill ingestion of Google Ads data for a specified date range.

    - **Parameters**:
        - backfill_request: Includes the start date, end date, and advertiser IDs.
    - **Returns**: JSON with a status message for success or no new data.
    - **Raises**: HTTPException with a status message for any ingestion failure.
    """
    try:
        return run_backfill_ingestion(
            backfill=True,
            start_date=backfill_request.start_date,
            end_date=backfill_request.end_date,
            advertiser_ids=backfill_request.advertiser_ids,
        )
    except HTTPException as http_exc:
        raise http_exc
    except Exception:
        logger.error("Unexpected error during backfill ingestion", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Unexpected error during backfill ingestion"
        )
