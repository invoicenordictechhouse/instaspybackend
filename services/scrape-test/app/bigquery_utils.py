# bigquery_utils.py

from google.cloud import bigquery
from models import BigQueryRow
from config_loader import config
from queries import GET_ROWS_QUERY
from utils import normalize_row_keys
from logging_config import logger
from typing import List, Dict, Any


def get_rows_from_bq(advertiser_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve rows from BigQuery for a given advertiser ID.

    Args:
        advertiser_id (str): The advertiser ID to query.

    Returns:
        List[Dict[str, Any]]: A list of rows matching the advertiser ID.
    """
    client = bigquery.Client(project=config["bigquery"]["project_id"])

    query = GET_ROWS_QUERY.format(
        table=config["bigquery"]["tables"]["test_consumption"]
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("advertiser_id", "STRING", advertiser_id)
        ]
    )

    try:
        logger.info(f"Executing BigQuery query for advertiser_id: {advertiser_id}")
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        raise

    rows = [normalize_row_keys(dict(row)) for row in results]
    logger.info(f"Fetched {len(rows)} rows for advertiser_id: {advertiser_id}")
    return rows


def insert_row_to_bq(row_data: BigQueryRow, destination_table: str) -> None:
    """
    Insert a row into a BigQuery table.

    Args:
        row_data (BigQueryRow): The data to insert.
        destination_table (str): The fully qualified table name.
    """
    client = bigquery.Client(project=config["bigquery"]["project_id"])

    row_dict = row_data.model_dump()

    logger.debug(f"Inserting row into {destination_table}: {row_dict}")

    errors = client.insert_rows_json(destination_table, [row_dict])
    if errors:
        logger.error(
            f"Errors occurred while inserting row into {destination_table}: {errors}"
        )
    else:
        logger.info(f"Row inserted into {destination_table} successfully.")
