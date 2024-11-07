from config import ADVERTISERS_TRACKING_TABLE_ID
from google.cloud import bigquery
from enums.IngestionStatus import IngestionStatus

from utils.check_table_row_count import check_table_row_count


def add_all_updated_ads(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    raw_table_id: str,
) -> IngestionStatus:
    """
    Inserts updated ad versions for all advertisers listed in the ADVERTISERS_TRACKING_TABLE_ID.

    This function retrieves the latest ad records for advertisers in the specified tracking table,
    and inserts new or modified ad records into the target raw table, ensuring only unique
    records are added based on raw data changes. This maintains historical versions of each ad.

    Args:
        bigquery_client (bigquery.Client): An instance of BigQuery client to execute queries.
        project_id (str): The Google Cloud project ID where BigQuery datasets reside.
        dataset_id (str): The ID of the dataset containing both the target and tracking tables.
        raw_table_id (str): The ID of the raw table where updated ad data is stored.

    Returns:
        IngestionStatus: An enum value indicating the status of the insertion process:
            - DATA_INSERTED: New rows were successfully added to the raw table.
            - NO_NEW_UPDATES: No new rows were added, as all ads already existed in the table.

    """
    initial_row_count = check_table_row_count(
        bigquery_client, project_id, dataset_id, raw_table_id
    )

    query = f"""
    INSERT INTO `{project_id}.{dataset_id}.{raw_table_id}` 
    (data_modified, metadata_time, advertiser_id, creative_id, raw_data)

    WITH filtered_ads AS (
        SELECT 
            TIMESTAMP(PARSE_DATE('%Y-%m-%d', (SELECT region.first_shown FROM UNNEST(t.region_stats) AS region WHERE region.region_code = "SE"))) AS data_modified,
            CURRENT_TIMESTAMP() AS metadata_time,
            t.advertiser_id,
            t.creative_id,
            TO_JSON_STRING(STRUCT(
                t.advertiser_id,
                t.creative_id,
                t.creative_page_url,
                t.ad_format_type,
                t.advertiser_disclosed_name,
                t.advertiser_legal_name,
                t.advertiser_location,
                t.advertiser_verification_status,
                t.topic,
                t.is_funded_by_google_ad_grants,
                ARRAY(
                    SELECT AS STRUCT *
                    FROM UNNEST(t.region_stats) AS region
                    WHERE region.region_code = "SE"
                ) AS region_stats,
                t.audience_selection_approach_info
            )) AS raw_data
        FROM
            `bigquery-public-data.google_ads_transparency_center.creative_stats` AS t
        WHERE 
            t.advertiser_id IN (SELECT advertiser_id FROM `{project_id}.{dataset_id}.{ADVERTISERS_TRACKING_TABLE_ID}`)
            AND t.advertiser_location = "SE"
            AND EXISTS (
                SELECT 1 FROM UNNEST(t.region_stats) AS region WHERE region.region_code = "SE"
            )
    )
    SELECT 
        data_modified,
        metadata_time,
        advertiser_id,
        creative_id,
        raw_data
    FROM
        filtered_ads
    WHERE NOT EXISTS (
        SELECT 1
        FROM `{project_id}.{dataset_id}.{raw_table_id}` AS existing
        WHERE existing.advertiser_id = ads_with_dates.advertiser_id
        AND existing.creative_id = ads_with_dates.creative_id
        AND existing.raw_data = ads_with_dates.raw_data
    )
    """
    query_job = bigquery_client.query(query)
    query_job.result()

    final_row_count = check_table_row_count(
        bigquery_client, project_id, dataset_id, raw_table_id
    )

    if final_row_count > initial_row_count:
        return IngestionStatus.DATA_INSERTED
    else:
        return IngestionStatus.NO_NEW_UPDATES
