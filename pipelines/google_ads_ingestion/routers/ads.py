import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException
from schemas.update_request import UpdateRequest
from enums.updateEnum import UpdateMode
from utils.process_google_ads_data_updates import process_google_ads_data_updates

router = APIRouter()

project_id = "annular-net-436607-t0"
dataset_id = "sample_ds"
table_id = "raw_sample_ads"
staging_table_id = "staging_active_sample_ids"
clean_table_id = "clean_google_ads"

@router.post(
    "/update",
    summary="Run Update",
    description="Triggers an update of Google Ads data based on the mode (ALL, ACTIVE, SPECIFIC).",
)
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
                detail="For SPECIFIC mode, either 'advertiser_ids' or 'creative_ids' must be provided.",
            )

    try:
        background_tasks.add_task(
            process_google_ads_data_updates,
            update_request.update_mode,
            project_id,
            dataset_id,
            table_id,
            staging_table_id,
            update_request.advertiser_ids,
            update_request.creative_ids,
        )

        return {
            "status": "Update initiated",
            "update_mode": update_request.update_mode,
            "description": f"Mode: {update_request.update_mode}",
        }

    except Exception as e:
        logging.error(f"Failed to initiate daily ingestion: {e}")
        raise HTTPException(status_code=500, detail="Daily ingestion failed")

