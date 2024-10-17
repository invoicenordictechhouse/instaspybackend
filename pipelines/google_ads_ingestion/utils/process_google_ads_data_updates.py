import logging
from google.cloud import bigquery

from .bigquery_client import bigquery_client
from .store_active_creative_ids_in_staging import store_active_creative_ids_in_staging
from .updateEnum import UpdateMode
from .update_active_ads import update_active_ads
from .update_specific_ads import update_specific_ads
from .update_all_adds import update_all_ads

staging_table_id = "staging_active_sample_ids"
clean_table_id = "clean_google_ads"
advertiser_ids_table = "advertiser_ids"


def process_google_ads_data_updates(
    update_mode: UpdateMode,
    project_id: str,
    dataset_id: str,
    raw_table_id: str,
    staging_table_id: str,
    advertiser_ids: list = None,
    creative_ids: list = None,
) -> None:
    try:
        if update_mode == UpdateMode.ACTIVE:
            store_active_creative_ids_in_staging(
                bigquery_client=bigquery_client,
                project_id=project_id,
                dataset_id=dataset_id,
                clean_table_id=clean_table_id,
                staging_table_id=staging_table_id,
            )
            update_active_ads(
                bigquery_client=bigquery_client,
                project_id=project_id,
                dataset_id=dataset_id,
                raw_table_id=raw_table_id,
                staging_table_id=staging_table_id,
            )
        elif update_mode == UpdateMode.ALL:
            update_all_ads(
                bigquery_client=bigquery_client,
                project_id=project_id,
                dataset_id=dataset_id,
                raw_table_id=raw_table_id,
                advertiser_ids_table=advertiser_ids_table,
            )
        elif update_mode == UpdateMode.SPECIFIC:
            update_specific_ads(
                bigquery_client=bigquery_client,
                project_id=project_id,
                dataset_id=dataset_id,
                raw_table_id=raw_table_id,
                advertiser_ids=advertiser_ids,
                creative_ids=creative_ids,
            )

    except Exception as e:
        logging.error(f"Failed to update existing ads: {e}")
        print(f"Failed to update existing ads: {e}")
        raise
