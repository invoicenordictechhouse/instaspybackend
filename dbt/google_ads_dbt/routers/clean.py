import logging
from typing import List
from fastapi import APIRouter, BackgroundTasks, HTTPException
from utils.run_dbt_command import run_dbt_command

router = APIRouter()

@router.post(
    "/all",
    summary="Clean All Advertisers",
    description="Trigger DBT model for all advertisers",
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
    except Exception as e:
        logging.error(f"Failed to initiate cleaning: {e}")
        raise HTTPException(status_code=500, detail="Cleaning failed")

@router.post(
    "/specific",
    summary="Clean Specific Advertisers",
    description="Trigger DBT model for specific advertisers by IDs",
)
async def clean_specific_advertiser(
    advertiser_ids: List[str], background_tasks: BackgroundTasks
):
    """
    Endpoint to trigger DBT model that cleans data for a specific advertiser.
    """
    if not advertiser_ids:
        raise HTTPException(status_code=400, detail="No advertiser_ids provided")

    try:
        ids_str = ",".join(advertiser_ids)
        background_tasks.add_task(
            run_dbt_command,
            f"dbt run --select clean --vars '{{advertiser_ids: [{ids_str}]}}'",
        )
        return {"status": f"Cleaning initiated for advertisers {advertiser_ids}"}
    except Exception as e:
        logging.error(
            f"Failed to initiate cleaning for advertisers {advertiser_ids}: {e}"
        )
        raise HTTPException(
            status_code=500, detail="Cleaning failed for the specified advertisers"
        )

