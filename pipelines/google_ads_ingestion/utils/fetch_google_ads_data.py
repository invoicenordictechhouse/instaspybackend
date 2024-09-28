from datetime import datetime, timedelta
import logging
from pytz import timezone
from .bigquery_client import bigquery_client
from .insert_incremental_google_ads_data import insert_incremental_google_ads_data

# Stockholm timezone
STOCKHOLM_TZ = timezone("Europe/Stockholm")


def fetch_google_ads_data(
    dataset_id: str,
    project_id: str,
    table_id: str,
    backfill=False,
    start_date=None,
    end_date=None,
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
        if not backfill:
            stockholm_time = datetime.now(STOCKHOLM_TZ)
            start_date = (stockholm_time - timedelta(days=1)
                          ).strftime("%Y-%m-%d")
            end_date = stockholm_time.strftime("%Y-%m-%d")

            logging.info(
                f"Fetching previous day's data: {start_date} to {end_date}")
        else:
            if not start_date or not end_date:
                raise ValueError(
                    "Please provide both start_date and end_date for backfill."
                )
            logging.info(f"Running backfill from {start_date} to {end_date}")

        insert_incremental_google_ads_data(
            bigquery_client, project_id, dataset_id, table_id, start_date, end_date
        )
    except Exception as e:
        logging.error(f"Failed to fetch Google Ads data: {e}")
