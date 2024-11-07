from typing import List
from google.cloud import bigquery
from enums.IngestionStatus import IngestionStatus
from utils.check_table_row_count import check_table_row_count


def add_targeted_ad_versions(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    raw_table_id: str,
    advertiser_ids: List[str] = None,
    creative_ids: List[str] = None,
) -> None:
    """
    Inserts new versions of ads for specified advertisers or creatives, retaining ad version history.

    This function targets specific ads identified by either `advertiser_ids` or `creative_ids` and inserts
    records into the raw table if there are updates. It prevents duplicate entries by checking for
    uniqueness based on raw data changes.

    Args:
        bigquery_client (bigquery.Client): BigQuery client instance for executing queries.
        project_id (str): Google Cloud project ID where the BigQuery dataset is located.
        dataset_id (str): The BigQuery dataset ID containing both the target and tracking tables.
        raw_table_id (str): The ID of the raw table where ad records are stored.
        advertiser_ids (List[str], optional): List of specific advertiser IDs to filter for updates.
        creative_ids (List[str], optional): List of specific creative IDs to filter for updates.

    Returns:
        IngestionStatus: Enum indicating the insertion status:
            - DATA_INSERTED: New records were added to the raw table.
            - NO_NEW_UPDATES: No new records were added as all ads already existed in the table.
    """
    conditions = []
    if advertiser_ids:
        advertiser_ids_str = ", ".join(
            [f'"{advertiser_id}"' for advertiser_id in advertiser_ids]
        )
        conditions.append(f"t.advertiser_id IN ({advertiser_ids_str})")

    if creative_ids:
        creative_ids_str = ", ".join(
            [f'"{creative_id}"' for creative_id in creative_ids]
        )
        conditions.append(f"t.creative_id IN ({creative_ids_str})")

    where_clause = " OR ".join(conditions)

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
            `bigquery-public-data.google_ads_transparency_center.creative_stats` AS t        WHERE 
            ({where_clause})
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
        WHERE existing.raw_data = filtered_ads.raw_data
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
