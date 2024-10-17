import logging

from .update_existing_google_ads_data import update_existing_google_ads_data
from .store_active_creative_ids_in_staging import store_active_creative_ids_in_staging
from .bigquery_client import bigquery_client
from .insert_new_google_ads_data import insert_new_google_ads_data


def fetch_google_ads_data(
    dataset_id: str,
    project_id: str,
    table_id: str,
    backfill:bool = False,
    start_date: str =None,
    end_date:str = None,
    advertiser_ids: list = None,
) -> None:
    """
    Fetch Google Ads data from BigQuery and insert it incrementally.

    If `backfill` is False, the function fetches the previous day's data. If `backfill` is True, it fetches
    data between `start_date` and `end_date`. The function inserts the data into BigQuery while avoiding duplicates.

    Parameters:
    dataset_id (str): BigQuery dataset ID.
    project_id (str): Google Cloud project ID.
    table_id (str): BigQuery table ID.
    backfill (bool): Whether to backfill data (default is False).
    start_date (str, optional): Start date for data fetching in 'YYYY-MM-DD' format (required for backfill).
    end_date (str, optional): End date for data fetching in 'YYYY-MM-DD' format (required for backfill).

    Raises:
    ValueError: If `backfill` is True and `start_date` or `end_date` is not provided.
    """
    try:
        if backfill:
            if not start_date or not end_date:
                raise ValueError("Both start_date and end_date must be provided for backfill.")
            logging.info(f"Backfilling data from {start_date} to {end_date}")
            insert_new_google_ads_data(
                bigquery_client=bigquery_client, project_id=project_id, dataset_id=dataset_id, table_id=table_id, backfill=backfill, start_date=start_date, end_date=end_date, advertiser_ids=advertiser_ids
            )
        else:
            insert_new_google_ads_data(
                bigquery_client=bigquery_client, project_id=project_id, dataset_id=dataset_id, table_id=table_id,backfill=backfill, start_date=start_date, end_date=end_date
            )
            
    except Exception as e:
        logging.error(f"Failed to fetch Google Ads data: {e}")
