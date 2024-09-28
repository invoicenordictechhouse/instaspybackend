import logging
from google.cloud import bigquery


def insert_incremental_google_ads_data(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Insert Google Ads Transparency data incrementally into BigQuery, avoiding duplicates.

    The function performs an incremental insert into BigQuery, selecting data within the date range specified by `start_date`
    and `end_date` and ensuring that no duplicates are added to the table.

    Parameters:
    bigquery_client (bigquery.Client): BigQuery client instance.
    project_id (str): Google Cloud project ID.
    dataset_id (str): BigQuery dataset ID.
    table_id (str): BigQuery table ID.
    start_date (str): Start date for data fetching in 'YYYY-MM-DD' format.
    end_date (str): End date for data fetching in 'YYYY-MM-DD' format.

    Raises:
    Exception: If the query execution or data insertion fails.
    """
    query = f"""
    INSERT INTO `{project_id}.{dataset_id}.{table_id}` 
    (data_modified, metadata_time, raw_data)
    SELECT
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d', region_stats.first_shown) AS data_modified,
        CURRENT_TIMESTAMP() AS metadata_time,
        TO_JSON_STRING(STRUCT(
            advertiser_id,
            creative_id,
            creative_page_url,
            ad_format_type,
            advertiser_disclosed_name,
            advertiser_legal_name,
            advertiser_location,
            advertiser_verification_status,
            TO_JSON_STRING(region_stats) AS region_stats_json,
            TO_JSON_STRING(audience_selection_approach_info) AS audience_selection_approach_info_json,
            topic,
            is_funded_by_google_ad_grants
        )) AS raw_data
    FROM
        `bigquery-public-data.google_ads_transparency_center.creative_stats`,
    UNNEST(region_stats) AS region_stats
    WHERE region_stats.first_shown BETWEEN '{start_date}' AND '{end_date}'
    -- Exclude records that are already in the table (incremental load logic)
    AND NOT EXISTS (
        SELECT 1 FROM `{project_id}.{dataset_id}.{table_id}` AS t
        WHERE t.raw_data LIKE CONCAT('%', advertiser_id, '%', creative_id, '%')
        AND t.data_modified = SAFE.PARSE_TIMESTAMP('%Y-%m-%d', region_stats.first_shown)
    )
    """
    try:
        query_job = bigquery_client.query(query)
        query_job.result()
        logging.info(f"Data inserted for range {start_date} to {end_date}")
    except Exception as e:
        print(f"Failed to insert data into BIgQuery: {e}")
        raise
