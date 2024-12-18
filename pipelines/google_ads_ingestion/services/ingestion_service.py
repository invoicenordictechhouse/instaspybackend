from fastapi.responses import JSONResponse
from utils.logging_config import logger
from typing import List, Union

from fastapi import HTTPException
from config import PROJECT_ID, DATASET_ID, RAW_TABLE_ID
from enums.IngestionStatus import IngestionStatus
from utils.create_incremental_table_if_not_exists import (
    create_incremental_table_if_not_exists,
)
from utils.insert_new_google_ads_data import insert_new_google_ads_data
from utils.bigquery_client import bigquery_client
from utils.handle_ingestion_result import handle_ingestion_result


async def run_daily_ingestion() -> JSONResponse:
    """
    Executes the daily ingestion process for Google Ads data.

    This function checks if the necessary BigQuery table exists or creates it if it doesn't.
    Then, it inserts new daily data into the specified table, ensuring only unique records are added.

    Raises:
        Exception: If the BigQuery table cannot be created or verified.
    """
    try:
        table_status = create_incremental_table_if_not_exists(
            bigquery_client, DATASET_ID, RAW_TABLE_ID
        )

        table_response = handle_ingestion_result(table_status, "Table verification")
        if table_status == IngestionStatus.TABLE_CREATION_FAILED:
            return table_response

        logger.info("Starting daily ingestion.")
        data_status = insert_new_google_ads_data(
            bigquery_client=bigquery_client,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=RAW_TABLE_ID,
            backfill=False,
        )

        return handle_ingestion_result(data_status, "Daily ingestion")

    except HTTPException as http_exc:
        raise http_exc

    except Exception:
        logger.error(
            "An unexpected error occurred during daily ingestion", exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred during daily ingestion.",
        )


async def run_backfill_ingestion(
    backfill: bool = False,
    start_date: str = None,
    end_date: str = None,
    advertiser_ids: Union[str, List[str]] = None,
) -> JSONResponse:
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
            raise HTTPException(
                status_code=400,
                detail="Advertiser IDs must be provided for targeted backfill.",
            )

        if isinstance(advertiser_ids, str):
            advertiser_ids = [advertiser_ids]

        if not start_date or not end_date:
            raise HTTPException(
                status_code=400,
                detail="Both start_date and end_date must be provided for backfill.",
            )

        table_status = create_incremental_table_if_not_exists(
            bigquery_client, DATASET_ID, RAW_TABLE_ID
        )

        table_response = handle_ingestion_result(table_status, "Table verification")
        if table_status == IngestionStatus.TABLE_CREATION_FAILED:
            return table_response

        logger.info(f"Starting backfill ingestion from {start_date} to {end_date}.")
        data_status = insert_new_google_ads_data(
            bigquery_client=bigquery_client,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=RAW_TABLE_ID,
            backfill=backfill,
            start_date=start_date,
            end_date=end_date,
            advertiser_ids=advertiser_ids,
        )
        return handle_ingestion_result(
            data_status, "Backfill ingestion", is_backfill=True
        )

    except Exception:
        logger.error(
            "An unexpected error occurred during backfill ingestion", exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred during backfill ingestion.",
        )
