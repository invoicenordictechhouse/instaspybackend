import logging
from utils.create_incremental_table_if_not_exists import (
    create_incremental_table_if_not_exists,
)
from utils.insert_new_google_ads_data import insert_new_google_ads_data
from utils.bigquery_client import bigquery_client
from config import PROJECT_ID, DATASET_ID, RAW_TABLE_ID


def run_daily_ingestion() -> None:
    """
    Executes the daily ingestion process for Google Ads data.

    This function checks if the necessary BigQuery table exists or creates it if it doesn't.
    Then, it inserts new daily data into the specified table, ensuring only unique records are added.

    Raises:
        Exception: If the BigQuery table cannot be created or verified.
    """
    try:
        if not create_incremental_table_if_not_exists(
            bigquery_client, DATASET_ID, RAW_TABLE_ID
        ):
            logging.error(
                f"Failed to create or verify table '{RAW_TABLE_ID}' in dataset '{DATASET_ID}'."
            )
            raise Exception(f"Table '{RAW_TABLE_ID}' could not be created or verified.")

        logging.info("Starting daily ingestion.")
        insert_new_google_ads_data(
            bigquery_client=bigquery_client,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=RAW_TABLE_ID,
            backfill=False,
        )
        logging.info("Daily ingestion completed successfully.")
    except Exception as e:
        logging.error(f"Daily ingestion failed: {e}")
        raise


def run_backfill_ingestion(
    backfill: bool = False,
    start_date: str = None,
    end_date: str = None,
    advertiser_ids: list = None,
) -> None:
    """
    Executes a backfill ingestion for Google Ads data over a specific date range.

    This function verifies or creates the required BigQuery table, then inserts historical data between
    the specified start and end dates, ensuring only unique records are added.

    Args:
        backfill (bool): Indicates that the function is performing a backfill ingestion.
        start_date (str): Start date for the backfill in 'YYYY-MM-DD' format.
        end_date (str): End date for the backfill in 'YYYY-MM-DD' format.
        advertiser_ids (list, optional): List of advertiser IDs to backfill data for.

    Raises:
        ValueError: If start_date or end_date or advertiser_ids is not provided when backfill is True.
        Exception: If the BigQuery table cannot be created or verified.
    """
    try:
        if not advertiser_ids:
            raise ValueError("Advertiser IDs must be provided for targeted backfill.")

        if not start_date or not end_date:
            raise ValueError(
                "Both start_date and end_date must be provided for backfill."
            )

        if not create_incremental_table_if_not_exists(
            bigquery_client, DATASET_ID, RAW_TABLE_ID
        ):
            raise Exception(f"Table '{RAW_TABLE_ID}' could not be created or verified.")

        logging.info(f"Starting backfill ingestion from {start_date} to {end_date}.")
        insert_new_google_ads_data(
            bigquery_client=bigquery_client,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=RAW_TABLE_ID,
            backfill=backfill,
            start_date=start_date,
            end_date=end_date,
            advertiser_ids=advertiser_ids,
        )
        logging.info("Backfill ingestion completed successfully.")

    except ValueError as ve:
        logging.error(f"Parameter validation error in backfill ingestion: {ve}")
        raise

    except Exception as e:
        logging.error(f"Backfill ingestion failed: {e}")
        raise
