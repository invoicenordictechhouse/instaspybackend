# ads_service/main.py
from fastapi import FastAPI, HTTPException, Query
from typing import List
from google.cloud import bigquery
from models import AdRecord
from bq_utils import query_ads_data
import logging

# Initialize FastAPI app
app = FastAPI()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# DONT FORGET TO CHANGE TABLE TO QUERY IN BQ_UTILS!!!!!
@app.get("/", summary="Root Endpoint")
def read_root():
    """
    Root endpoint providing a welcome message.

    Returns:
        dict: A welcome message.
    """
    return {
        "message": "Welcome to the Ads Data API! Use the /ads endpoint to retrieve data."
    }


@app.get(
    "/ads",
    response_model=List[AdRecord],
    summary="Retrieve Ads by Advertiser Disclosed Name",
)
async def get_ads(
    advertiser_disclosed_name: str = Query(
        ..., min_length=1, description="Name of the advertiser to search for"
    ),
    limit: int = Query(10, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
):
    """
    Endpoint to retrieve ads by advertiser disclosed name with optional pagination.

    Args:
        advertiser_disclosed_name (str): The name of the advertiser to search for.
        limit (int): Number of records to return.
        offset (int): Number of records to skip.

    Returns:
        List[AdRecord]: A list of ads that match the advertiser name.

    Raises:
        HTTPException: If no matching records are found or if there’s a server error.
    """
    try:
        logger.info(f"Querying ads for advertiser: {advertiser_disclosed_name}")
        rows = query_ads_data(advertiser_disclosed_name, limit, offset)
        if not rows:
            raise HTTPException(status_code=404, detail="No matching records found.")
        return rows
    except HTTPException as he:
        logger.error(f"HTTP exception: {he.detail}")
        raise he
    except Exception as e:
        logger.error(f"Error querying ads: {e}")
        raise HTTPException(status_code=500, detail=str(e))
