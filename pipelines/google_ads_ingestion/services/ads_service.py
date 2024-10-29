import logging
from config import PROJECT_ID, DATASET_ID, RAW_TABLE_ID
from utils.bigquery_client import bigquery_client
from enums.InsertionEnum import InsertionMode
from utils.add_targeted_ad_versions import add_targeted_ad_versions
from utils.add_all_updated_ads import add_all_updated_ads


def run_ads_insertion(
    insertion_mode: InsertionMode,
    advertiser_ids: list = None,
    creative_ids: list = None,
) -> None:
    """
    Handles Google Ads data insertion based on the specified mode.

    Args:
        advertiser_ids (list, optional): List of specific advertiser IDs to update (for SPECIFIC mode).
        creative_ids (list, optional): List of specific creative IDs to update (for SPECIFIC mode).

    Raises:
        ValueError: If advertisers_ids or creative_ids parameters are missing.
        Exception: If an error occurs during the update process.
    """
    try:
        if insertion_mode == InsertionMode.ALL:
            if not advertiser_ids and not creative_ids:
                raise ValueError(
                    "For SPECIFIC mode, either 'advertiser_ids' or 'creative_ids' must be provided."
                )

            logging.info(f"Starting update for ALL ads in {RAW_TABLE_ID}.")
            add_all_updated_ads(
                bigquery_client=bigquery_client,
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                raw_table_id=RAW_TABLE_ID,
            )
            logging.info("Completed update for ALL ads.")

        if insertion_mode == InsertionMode.SPECIFIC:
            logging.info(
                f"Starting SPECIFIC update for advertiser_ids: {advertiser_ids} and creative_ids: {creative_ids}"
            )
            add_targeted_ad_versions(
                bigquery_client=bigquery_client,
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                raw_table_id=RAW_TABLE_ID,
                advertiser_ids=advertiser_ids,
                creative_ids=creative_ids,
            )
            logging.info("Completed SPECIFIC update.")

    except ValueError as ve:
        logging.error(f"Parameter validation error in insert ads: {ve}")
        raise

    except Exception as e:
        logging.error(f"Failed to insert updated ads: {e}")
        raise
