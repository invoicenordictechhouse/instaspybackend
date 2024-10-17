import logging
from typing import List
from google.cloud import bigquery


def update_specific_ads(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    raw_table_id: str,
    advertiser_ids: List[str] = None,
    creative_ids: List[str] = None,
) -> None:
    """
    Update specific Google Ads data based on provided advertiser_ids and/or creative_ids.

    Args:
    bigquery_client (bigquery.Client): BigQuery client instance.
    project_id (str): Google Cloud project ID.
    dataset_id (str): BigQuery dataset ID.
    raw_table_id (str): Raw table ID to update.
    advertiser_ids (List[str], optional): List of advertiser IDs to update.
    creative_ids (List[str], optional): List of creative IDs to update.

    Raises:
    Exception: If the query execution or data update fails.
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

    if not conditions:
        raise ValueError(
            "At least one of advertiser_ids or creative_ids must be provided."
        )

    where_clause = " OR ".join(conditions)

    query = f"""
    WITH filtered_ads AS (
        SELECT 
            PARSE_DATE('%Y-%m-%d', (SELECT region.first_shown FROM UNNEST(t.region_stats) AS region WHERE region.region_code = "SE")) AS data_modified,
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
                ) AS region_stats
            )) AS raw_data
        FROM
            `bigquery-public-data.google_ads_transparency_center.creative_stats` AS t,
            UNNEST(t.region_stats) AS region
        WHERE ({where_clause})
    )
    INSERT INTO `{project_id}.{dataset_id}.{raw_table_id}` 
    (data_modified, metadata_time, advertiser_id, creative_id, raw_data)
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
        WHERE existing.advertiser_id = filtered_ads.advertiser_id
        AND existing.creative_id = filtered_ads.creative_id
        AND existing.raw_data = filtered_ads.raw_data
    )
    """

    try:
        query_job = bigquery_client.query(query)
        query_job.result()
        logging.info(
            f"Specific ads updated successfully based on advertiser_ids: {advertiser_ids} or creative_ids: {creative_ids}"
        )
        print(
            f"Specific ads updated successfully based on advertiser_ids: {advertiser_ids} or creative_ids: {creative_ids}"
        )

    except Exception as e:
        logging.error(f"Failed to update specific ads: {e}")
        print(f"Failed to update specific ads: {e}")
        raise
