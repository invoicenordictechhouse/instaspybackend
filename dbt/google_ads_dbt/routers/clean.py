import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException
from schemas.AdvertiserRequest import AdvertiserRequest
from utils.run_dbt_command import run_dbt_command

router = APIRouter()


@router.post(
    "/all",
    summary="Clean All Advertisers",
    description="Trigger DBT model Clean for all advertisers",
    responses={
        200: {"description": "Cleaning initiated successfully", "content": {"application/json": {"example": {"status": "Cleaning initiated for all advertisers (NO_ID)"}}}},
        400: {"description": "Bad Request - No advertiser_ids provided", "content": {"application/json": {"example": {"detail": "No advertiser_ids provided"}}}},
        500: {"description": "Internal Server Error", "content": {"application/json": {"example": {"detail": "An unexpected error occurred"}}}},
    }
)
async def clean_all_advertisers(background_tasks: BackgroundTasks):
    """
    Endpoint to trigger DBT model that cleans data for all advertisers (uses NO_ID as placeholder).
    """
    try:
        background_tasks.add_task(
            run_dbt_command, "dbt run --select clean --vars '{advertiser_id: NO_ID}'"
        )
        return {"status": "Cleaning initiated for all advertisers (NO_ID)"}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Failed to initiate cleaning: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")


@router.post(
    "/specific",
    summary="Clean Specific Advertisers",
    description="Trigger DBT model Clean for specific advertisers by IDs",
    responses={
        200: {"description": "Cleaning initiated successfully", "content": {"application/json": {"example": {"status": "Cleaning initiated for advertisers ['12345', '67890']"}}}},
        400: {"description": "Bad Request - No advertiser_ids provided", "content": {"application/json": {"example": {"detail": "No advertiser_ids provided"}}}},
        500: {"description": "Internal Server Error", "content": {"application/json": {"example": {"detail": "An unexpected error occurred"}}}},
    }
)
async def clean_specific_advertiser(
    request: AdvertiserRequest, background_tasks: BackgroundTasks
):
    """
    Endpoint to trigger DBT model that cleans data for a specific advertiser.

    - **advertiser_ids**: Accepts a single advertiser ID as a string or multiple IDs as a list.
    """
    advertiser_ids = request.advertiser_ids
    if isinstance(advertiser_ids, str):
        ids_str = advertiser_ids
    elif isinstance(advertiser_ids, list) and advertiser_ids:
        ids_str = ",".join(advertiser_ids)
    else:
        raise HTTPException(status_code=400, detail="No valid advertiser_ids provided")

    try:
        background_tasks.add_task(
            run_dbt_command,
            f"dbt run --select clean --vars '{{advertiser_ids: [{ids_str}]}}'",
        )
        return {"status": f"Cleaning initiated for advertisers {advertiser_ids}"}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Failed to initiate cleaning on advertiser {advertiser_ids}: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")


