import datetime
from decimal import Decimal
from typing import List
import re
from google.cloud import bigquery
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

app = FastAPI()

# Initialize BigQuery client
client = bigquery.Client()


# Define a Pydantic model for the response
class AdRecord(BaseModel):
    advertiser_id: str = None
    creative_id: str = None
    creative_page_url: str = None
    ad_format_type: str = None
    advertiser_disclosed_name: str = None
    advertiser_location: str = None
    topic: str = None
    is_funded_by_google_ad_grants: bool = None
    region_code: str = None
    first_shown: str = None
    last_shown: str = None
    times_shown_start_date: str = None
    times_shown_end_date: str = None
    youtube_times_shown_lower_bound: int = None
    youtube_times_shown_upper_bound: int = None
    search_times_shown_lower_bound: int = None
    search_times_shown_upper_bound: int = None
    shopping_times_shown_lower_bound: int = None
    shopping_times_shown_upper_bound: int = None
    maps_times_shown_lower_bound: int = None
    maps_times_shown_upper_bound: int = None
    play_times_shown_lower_bound: int = None
    play_times_shown_upper_bound: int = None
    youtube_video_url: str = None
    youtube_watch_url: str = None


class AdQuery(BaseModel):
    advertiser_disclosed_name: str = Field(
        ..., min_length=1, description="Name of the advertiser to search for"
    )


@app.get("/", summary="Root Endpoint")
def read_root():
    return {
        "message": "Welcome to the Ads Data API! Use the /ads endpoint to retrieve data."
    }


@app.get(
    "/ads",
    response_model=List[AdRecord],
    summary="Retrieve Ads by Advertiser Disclosed Name",
)
def get_ads(
    advertiser_disclosed_name: str = Query(
        ..., min_length=1, description="Name of the advertiser to search for"
    ),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
):
    # Sanitize input to prevent SQL injection
    sanitized_name = advertiser_disclosed_name.replace("'", "\\'")

    query = f"""
        SELECT *
        FROM `annular-net-436607-t0.sample_ds.new_table_with_youtube_link`
        WHERE LOWER(advertiser_disclosed_name) = LOWER(@advertiser_disclosed_name)
        LIMIT @limit OFFSET @offset
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "advertiser_disclosed_name", "STRING", sanitized_name
            ),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

        # Convert results to list of dictionaries
        rows = [dict(row) for row in results]

        if not rows:
            raise HTTPException(status_code=404, detail="No matching records found.")

        # Serialize date and datetime fields to string
        for row in rows:
            for key, value in row.items():
                if isinstance(value, (datetime.date, datetime.datetime)):
                    row[key] = value.isoformat()

        return rows

    except HTTPException as he:
        raise he  # Re-raise HTTP exceptions
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
