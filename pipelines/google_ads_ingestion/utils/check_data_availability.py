from google.cloud import bigquery
from typing import List
from .query_builder import QueryBuilder


def check_data_availability(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    advertiser_ids_table: str,
    start_date: str,
    end_date: str,
    advertiser_ids: List[str] = None,
    backfill: bool = False,
) -> bool:
    """
    Verifies if relevant ad data exists within the specified date range for targeted advertisers.

    This function checks if there is any available data for specific advertisers within a
    specified date range in the Google Ads Transparency dataset. The search can be based on
    advertiser IDs provided in the function call (`backfill=True`) or based on IDs present in
    an advertiser tracking table.

    Args:
        bigquery_client (bigquery.Client): BigQuery client instance for executing queries.
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID where the tables reside.
        advertiser_ids_table (str): Table ID for tracking advertiser IDs.
        start_date (str): Start date for the data range (YYYY-MM-DD format).
        end_date (str): End date for the data range (YYYY-MM-DD format).
        advertiser_ids (List[str], optional): List of specific advertiser IDs for targeted data retrieval (used if backfill=True).
        backfill (bool): Flag indicating if specific advertiser IDs are used (True) or all advertiser IDs from the table (False).

    Returns:
        bool: True if relevant data is found within the specified range, otherwise False.
    """

    query = QueryBuilder.build_check_data_availability_query(
        project_id=project_id,
        dataset_id=dataset_id,
        advertiser_ids_table=advertiser_ids_table,
        backfill=backfill,
    )

    query_params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
    ]
    if backfill:
        query_params.append(
            bigquery.ArrayQueryParameter("advertiser_ids", "STRING", advertiser_ids)
        )

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    check_job = bigquery_client.query(query, job_config=job_config)
    return check_job.result().total_rows > 0
