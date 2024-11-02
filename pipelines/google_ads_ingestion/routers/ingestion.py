from fastapi import APIRouter, BackgroundTasks, HTTPException
from schemas.BackfillRequest import BackfillRequest
from services.ingestion_service import run_daily_ingestion, run_backfill_ingestion

router = APIRouter()


@router.post(
    "/daily",
    summary="Run Daily Ingestion",
    description="Triggers the daily ingestion of Google Ads data for the previous day, ensuring the latest data is inserted into BigQuery.",
    responses={
        200: {
            "description": "Daily ingestion completed successfully with new data added.",
            "content": {"application/json": {"example": {"status": "Daily ingestion completed successfully"}}},
        },
        204: {
            "description": "No new data available for ingestion; table is up-to-date.",
            "content": {"application/json": {"example": {"status": "No data available for ingestion"}}},
        },
        500: {
            "description": "An unexpected error occurred during ingestion.",
            "content": {"application/json": {"example": {"detail": "An unexpected error occurred during ingestion"}}},
        },
    }
)
async def daily_ingestion(background_tasks: BackgroundTasks):
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
        background_tasks.add_task(run_daily_ingestion)
        return {"status": "Daily ingestion initiated"}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail={"status": "Unexpected error during daily ingestion"})


@router.post(
    "/backfill",
    summary="Run Backfill Ingestion",
    description="Triggers a backfill ingestion for a specified date range.",
    response_model=BackfillRequest,
    responses={
        200: {
            "description": "Backfill ingestion completed successfully or no new rows added.",
            "content": {"application/json": {"example": {"status": "Backfill ingestion completed successfully with new data added"}}},
        },
        204: {
            "description": "No data available for ingestion.",
            "content": {"application/json": {"example": {"status": "No data available for ingestion"}}},
        },
        400: {
            "description": "Bad request, missing parameters.",
            "content": {"application/json": {"example": {"status": "Advertiser IDs, start_date, and end_date must be provided for targeted backfill."}}},
        },
        500: {
            "description": "Error during ingestion",
            "content": {"application/json": {"example": {"status": "An unexpected error occurred during backfill ingestion"}}},
        },
    }
)
async def backfill_ingestion(
    backfill_request: BackfillRequest, background_tasks: BackgroundTasks
):
    """
    This endpoint triggers a backfill ingestion of Google Ads data for a specified date range.

    - **Parameters**:
        - backfill_request: Includes the start date, end date, and advertiser IDs.
    - **Returns**: JSON with a status message for success or no new data.
    - **Raises**: HTTPException with a status message for any ingestion failure.
    """
    try:
        background_tasks.add_task(
            run_backfill_ingestion,
            True,
            backfill_request.start_date,
            backfill_request.end_date,
            backfill_request.advertiser_ids,
        )
        return {"status": "Backfill initiated", "details": backfill_request.model_dump()}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail="Unexpected error during backfill ingestion")

