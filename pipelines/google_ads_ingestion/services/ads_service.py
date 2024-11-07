from fastapi.responses import JSONResponse
from utils.logging_config import logger
from fastapi import HTTPException
from config import PROJECT_ID, DATASET_ID, RAW_TABLE_ID
from utils.bigquery_client import bigquery_client
from enums.InsertionEnum import InsertionMode
from utils.add_targeted_ad_versions import add_targeted_ad_versions
from utils.add_all_updated_ads import add_all_updated_ads
from utils.handle_ingestion_result import handle_ingestion_result


def run_ads_insertion(
    insertion_mode: InsertionMode,
    advertiser_ids: list = None,
    creative_ids: list = None,
) -> JSONResponse:
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
        if insertion_mode == InsertionMode.SPECIFIC and not (
            advertiser_ids or creative_ids
        ):
            raise HTTPException(
                status_code=400,
                detail="In SPECIFIC mode, 'advertiser_ids' or 'creative_ids' is required.",
            )

        if insertion_mode == InsertionMode.ALL:
            logger.info(f"Starting update for ALL ads in {RAW_TABLE_ID}.")
            result = add_all_updated_ads(
                bigquery_client=bigquery_client,
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                raw_table_id=RAW_TABLE_ID,
            )
            return handle_ingestion_result(result, "ALL ads update")

        if insertion_mode == InsertionMode.SPECIFIC:
            logger.info(
                f"Starting SPECIFIC update for advertiser_ids: {advertiser_ids} and creative_ids: {creative_ids}"
            )
            result = add_targeted_ad_versions(
                bigquery_client=bigquery_client,
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                raw_table_id=RAW_TABLE_ID,
                advertiser_ids=advertiser_ids,
                creative_ids=creative_ids,
            )

            return handle_ingestion_result(result, "SPECIFIC ads update")

    except HTTPException as http_exc:
        raise http_exc

    except Exception:
        logger.error("An unexpected error occurred during ad insertion", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An unexpected error occurred during ad insertion."
        )
