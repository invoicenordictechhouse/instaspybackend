import logging
from typing import List
from fastapi import APIRouter, BackgroundTasks, HTTPException
from utils.run_dbt_command import run_dbt_command
from schemas.AdvertiserRequest import AdvertiserRequest

router = APIRouter()


@router.post(
    "/all",
    summary="Run Base Consume for All Advertisers",
    description="Trigger DBT model for base consume processing for all advertisers",
    responses={
        200: {"description": "Base consume processing initiated successfully", "content": {"application/json": {"example": {"status": "Base consume processing initiated for all advertisers (NO_ID)"}}}},
        500: {"description": "Internal Server Error", "content": {"application/json": {"example": {"detail": "Base consume processing failed"}}}},
    }
)
async def base_consume_all_advertisers(background_tasks: BackgroundTasks):
    """
    Endpoint to trigger the DBT model that processes base consume data for all advertisers (uses NO_ID as placeholder).
    """
    try:
        background_tasks.add_task(
            run_dbt_command, "dbt run --select base_consume --vars '{advertiser_id: NO_ID}'"
        )
        return {"status": "Base consume processing initiated for all advertisers (NO_ID)"}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Failed to initiate base consume processing: {e}")
        raise HTTPException(status_code=500, detail="Base consume processing failed")



@router.post(
    "/specific",
    summary="Run Base Consume for Specific Advertisers",
    description="Trigger DBT model for base consume processing for specific advertisers by IDs",
    responses={
        200: {"description": "Base consume processing initiated successfully", "content": {"application/json": {"example": {"status": "Base consume processing initiated for advertisers ['12345', '67890']"}}}},
        400: {"description": "Bad Request - No advertiser_ids provided", "content": {"application/json": {"example": {"detail": "No advertiser_ids provided"}}}},
        500: {"description": "Internal Server Error", "content": {"application/json": {"example": {"detail": "Base consume processing failed for the specified advertisers"}}}},
    }
)
async def base_consume_specific_advertiser(
    request: AdvertiserRequest, background_tasks: BackgroundTasks
):
    """
    Endpoint to trigger the DBT model that processes base consume data for specific advertisers.
    
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
            f"dbt run --select base_consume --vars '{{advertiser_ids: [{ids_str}]}}'",
        )
        return {"status": f"Base consume processing initiated for advertisers {advertiser_ids}"}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(
            f"Failed to initiate base consume processing for advertisers {advertiser_ids}: {e}"
        )
        raise HTTPException(
            status_code=500, detail="Base consume processing failed for the specified advertisers"
        )


