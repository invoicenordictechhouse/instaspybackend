import logging
from google.cloud import bigquery


def update_active_ads(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    raw_table_id: str,
    staging_table_id: str,
) -> None:
    """
    Update existing Google Ads data for active ads flagged in the staging table.

    Args:
    bigquery_client (bigquery.Client): BigQuery client instance.
    project_id (str): Google Cloud project ID.
    dataset_id (str): BigQuery dataset ID.
    raw_table_id (str): Raw table ID to update.
    staging_table_id (str): Temporary staging table ID containing active creative_ids.

    Raises:
    Exception: If the query execution or data update fails.
    """
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
                ) AS region_stats
            )) AS raw_data
        FROM
            `bigquery-public-data.google_ads_transparency_center.creative_stats` AS t,
            UNNEST(t.region_stats) AS region
        WHERE
            t.creative_id IN (SELECT creative_id FROM `{project_id}.{dataset_id}.{staging_table_id}`)
            AND t.advertiser_location = "SE"
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
        WHERE existing.advertiser_id = filtered_ads.advertiser_id
        AND existing.creative_id = filtered_ads.creative_id
        AND existing.raw_data = filtered_ads.raw_data
    )
    """

    try:
        query_job = bigquery_client.query(query)
        query_job.result()
        bigquery_client.query(
            f"DROP TABLE IF EXISTS `{project_id}.{dataset_id}.{staging_table_id}`"
        ).result()
        logging.info("Existing data updated for active ads in staging table")

    except Exception as e:
        logging.error(f"Failed to update existing ads: {e}")
        raise
