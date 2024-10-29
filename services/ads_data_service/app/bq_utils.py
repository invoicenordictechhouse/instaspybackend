# ads_service/bigquery_service.py
from google.cloud import bigquery
from typing import List, Dict, Any
import logging
import datetime
from urllib.parse import urlparse
import requests
import re

client = bigquery.Client()
logger = logging.getLogger(__name__)


def sanitize_input(input_string: str) -> str:
    """
    Sanitizes input to prevent SQL injection.

    Args:
        input_string (str): The input string to sanitize.

    Returns:
        str: The sanitized string.
    """
    return input_string.replace("'", "\\'")


def serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Serializes a row dictionary by converting date fields to ISO format strings.

    Args:
        row (Dict[str, Any]): The row data to serialize.

    Returns:
        Dict[str, Any]: The serialized row with date fields as ISO strings.
    """
    for key, value in row.items():
        if isinstance(value, (datetime.date, datetime.datetime)):
            row[key] = value.isoformat()
    return row


def query_ads_data(
    advertiser_name: str, limit: int, offset: int
) -> List[Dict[str, Any]]:
    """
    Queries BigQuery to retrieve ads by advertiser disclosed name.

    Args:
        advertiser_name (str): The name of the advertiser to search for.
        limit (int): Number of records to return.
        offset (int): Number of records to skip.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing an ad record.
    """
    sanitized_name = sanitize_input(advertiser_name)
    query = """
        SELECT 
            advertiser_disclosed_name,
            advertiser_location,
            region_code,
            topic,
            first_shown,
            last_shown,
            youtube_times_shown_lower_bound,
            youtube_times_shown_upper_bound,
            search_times_shown_lower_bound,
            search_times_shown_upper_bound,
            shopping_times_shown_lower_bound,
            shopping_times_shown_upper_bound,
            maps_times_shown_lower_bound,
            maps_times_shown_upper_bound,
            play_times_shown_lower_bound,
            play_times_shown_upper_bound,
            youtube_watch_url
        FROM 
            `annular-net-436607-t0.sample_ds.new_table_with_youtube_links`
        WHERE 
            LOWER(advertiser_disclosed_name) = LOWER(@advertiser_disclosed_name)
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

        # Convert results to list of dictionaries and serialize dates
        rows = [serialize_row(dict(row)) for row in results]
        return rows
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        raise
