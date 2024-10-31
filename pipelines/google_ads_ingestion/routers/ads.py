import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException
from schemas.InsertionRequest import InsertionRequest
from services.ads_service import run_ads_insertion

router = APIRouter()


@router.post(
    "/insert-updated",
    summary="Insert updated ads",
    description="Inserts Google Ads data that has been updated or modified.",
)
async def insert_updated_ads(
    insertion_request: InsertionRequest, background_tasks: BackgroundTasks
):
    """
    Triggers the insertion of updated Google Ads data.

    This endpoint accepts an insertion mode, either updating all ads or targeting specific ads based on provided
    advertiser or creative IDs. The insertion runs as a background task to ensure the API remains responsive.

    Parameters:
        insertion_request (InsertionRequest): The request body containing the insertion mode (ALL or SPECIFIC) and
                                              optional advertiser_ids or creative_ids for targeted updates.

    Background Task:
        Adds a background task to execute the ad insertion process via `run_ads_insertion`.

    Raises:
        HTTPException: If no advertiser or creative IDs are provided for SPECIFIC mode, or if the insertion process fails.

    Returns:
        dict: A message indicating that the insertion process has been initiated, along with the selected update mode.
    """
    try:
        background_tasks.add_task(
            run_ads_insertion,
            insertion_request.insertion_mode,
            insertion_request.advertiser_ids,
            insertion_request.creative_ids,
        )

        return {"status": "Insertion initiated", "details": InsertionRequest.model_dump()}


    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Failed to add updated ads: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error during adding new ads update")
