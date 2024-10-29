from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from models import AdRecord
from bq_utils import query_ads_data
from url_validator import check_url
import logging
import httpx
import asyncio
from google.cloud import bigquery
from fetch_secret import get_secret


# Initialize FastAPI app and logging
app = FastAPI()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
client = bigquery.Client()

# Set your scraping and dbt workflow URLs
PROJECT_ID = "your_project_id"
SCRAPING_SERVICE_URL = get_secret("SCRAPING_SERVICE_URL", PROJECT_ID)
DBT_WORKFLOW_URL = get_secret("DBT_WORKFLOW_URL", PROJECT_ID)


async def trigger_scraping_service(company_url: str):
    """
    Triggers the external scraping service if the company URL is not tracked.
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(
            SCRAPING_SERVICE_URL, json={"company_url": company_url}
        )
        if response.status_code == 200:
            logger.info(
                f"Scraping service triggered successfully for URL: {company_url}"
            )
        else:
            logger.error(
                f"Failed to trigger scraping service. Status: {response.status_code}"
            )
            raise HTTPException(
                status_code=500, detail="Error triggering scraping service."
            )


async def trigger_dbt_workflow(advertiser_id: str):
    """
    Triggers a dbt workflow to ingest ads data for the given advertiser ID.
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(
            DBT_WORKFLOW_URL, json={"advertiser_id": advertiser_id}
        )
        if response.status_code == 200:
            logger.info(
                f"DBT workflow triggered successfully for advertiser ID: {advertiser_id}"
            )
        else:
            logger.error(
                f"Failed to trigger DBT workflow. Status: {response.status_code}"
            )
            raise HTTPException(
                status_code=500, detail="Error triggering DBT workflow."
            )


def check_tracked_company(company_url: str) -> Optional[str]:
    """
    Checks if the company URL is already tracked in the `tracked_companies` table.
    If tracked, returns the advertiser ID. Otherwise, returns None.
    """
    query = """
        SELECT advertiser_id
        FROM `your_project.your_dataset.tracked_companies`
        WHERE company_url = @company_url
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("company_url", "STRING", company_url)
        ]
    )
    results = client.query(query, job_config=job_config).result()
    row = next(results, None)
    return row["advertiser_id"] if row else None


@app.get("/", summary="Root Endpoint")
def read_root():
    """
    Root endpoint providing a basic welcome message.

    Returns:
        dict: A welcome message or basic information about the API.
    """
    return {
        "message": "Welcome to the Ads Data API! Use the /ads endpoint to retrieve data."
    }


@app.get("/ads", response_model=List[AdRecord], summary="Retrieve Ads by Company URL")
async def get_ads_by_url(
    company_url: str = Query(..., description="Company URL to check or track"),
    limit: int = Query(10, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
):
    """
    Endpoint to retrieve ads by company URL. If the URL is not tracked, triggers
    a scraping service, followed by a dbt workflow to ingest ads data.
    """
    try:
        # Step 1: Validate and normalize the URL
        logger.info(f"Validating company URL: {company_url}")
        normalized_url = check_url(company_url)

        # Step 2: Check if URL exists in tracked companies
        advertiser_id = check_tracked_company(normalized_url)

        if not advertiser_id:
            logger.info(
                f"URL not found in tracked companies, triggering scraping service."
            )
            await trigger_scraping_service(normalized_url)

            # Poll until scraping is complete and data is available
            for _ in range(10):  # Poll up to 10 times with a delay
                await asyncio.sleep(3)  # Wait before re-checking
                advertiser_id = check_tracked_company(normalized_url)
                if advertiser_id:
                    break
            if not advertiser_id:
                raise HTTPException(
                    status_code=404,
                    detail="Data collection in progress. Please try again later.",
                )

            # Step 3: Trigger the dbt workflow to ingest ads data for the advertiser
            logger.info(f"Triggering dbt workflow for advertiser ID: {advertiser_id}")
            await trigger_dbt_workflow(advertiser_id)

            # Poll until ads data is ingested
            for _ in range(10):  # Poll up to 10 times with a delay
                await asyncio.sleep(3)  # Wait before re-checking
                ads_data = query_ads_data(advertiser_id, limit, offset)
                if ads_data:
                    break
            if not ads_data:
                raise HTTPException(
                    status_code=404,
                    detail="Ads data ingestion in progress. Please try again later.",
                )
        else:
            # Retrieve ads data if advertiser_id is already tracked
            logger.info(f"Retrieving ads for advertiser ID: {advertiser_id}")
            ads_data = query_ads_data(advertiser_id, limit, offset)
            if not ads_data:
                raise HTTPException(
                    status_code=404, detail="No ads found for the given advertiser."
                )

        return ads_data

    except HTTPException as he:
        logger.error(f"HTTP exception: {he.detail}")
        raise he
    except Exception as e:
        logger.error(f"Error querying ads: {e}")
        raise HTTPException(status_code=500, detail=str(e))
