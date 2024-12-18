from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
from typing import List
from .query_builder import QueryBuilder
from .check_data_availability import check_data_availability
from .check_table_row_count import check_table_row_count
from enums.IngestionStatus import IngestionStatus
from config import ADVERTISERS_TRACKING_TABLE_ID


def insert_new_google_ads_data(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    advertiser_ids_table: str = ADVERTISERS_TRACKING_TABLE_ID,
    backfill: bool = False,
    start_date: str = None,
    end_date: str = None,
    advertiser_ids: List = None,
) -> IngestionStatus:
    """
    Insert Google Ads Transparency data incrementally into BigQuery, avoiding duplicates.

    This function inserts data within the given date range (`start_date` to `end_date`)
    while avoiding duplicates by ensuring that the combination of `advertiser_id`, `creative_id`,
    and `data_modified` (first_shown) does not already exist.

    Parameters:
    bigquery_client (bigquery.Client): BigQuery client instance.
    project_id (str): Google Cloud project ID.
    dataset_id (str): BigQuery dataset ID.
    table_id (str): BigQuery table ID.
    start_date (str): Start date for data fetching in 'YYYY-MM-DD' format.
    end_date (str): End date for data fetching in 'YYYY-MM-DD' format.

    Raises:
    Exception: If the query execution or data insertion fails.
    """

    if not backfill:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        start_date = target_date
        end_date = target_date

    data_available = check_data_availability(
        bigquery_client,
        project_id,
        dataset_id,
        advertiser_ids_table,
        start_date,
        end_date,
        advertiser_ids,
        backfill,
    )

    if not data_available:
        return IngestionStatus.NO_DATA_AVAILABLE

    initial_row_count = check_table_row_count(
        bigquery_client, project_id, dataset_id, table_id
    )

    query = QueryBuilder.build_insert_new_google_ads_data_query(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
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

    query_job = bigquery_client.query(query, job_config=job_config)
    query_job.result()

    final_row_count = check_table_row_count(
        bigquery_client, project_id, dataset_id, table_id
    )

    if final_row_count > initial_row_count:
        return IngestionStatus.DATA_INSERTED
    else:
        return IngestionStatus.INCOMPLETE_INSERTION
