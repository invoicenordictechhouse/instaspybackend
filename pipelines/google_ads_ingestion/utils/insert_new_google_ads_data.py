import logging
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone


def insert_new_google_ads_data(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    advertiser_ids_table: str = "advertisers_tracking",
    backfill: bool = False,
    start_date: str = None,
    end_date: str = None,
    advertiser_ids: list = None,
) -> None:
    """
    Insert Google Ads Transparency data incrementally into BigQuery, avoiding duplicates.

    This function inserts data within the given date range (`start_date` to `end_date`)
    while avoiding duplicates by ensuring that the combination of `advertiser_id`, `creative_id`,
    and `data_modified` (first_shown) does not already exist.

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

    if not backfill:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        start_date = target_date
        end_date = target_date

    query = f"""
    INSERT INTO `{project_id}.{dataset_id}.{table_id}` (data_modified, metadata_time, advertiser_id, creative_id, raw_data)
    WITH selected_advertisers AS (
        {"SELECT x AS advertiser_id FROM UNNEST(@advertiser_ids) AS x" 
         if backfill else f"SELECT advertiser_id FROM `{project_id}.{dataset_id}.{advertiser_ids_table}`"}
    ),
    ads_with_dates AS (
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
            t.advertiser_id IN (SELECT advertiser_id FROM selected_advertisers)
            AND t.advertiser_location = "SE"
            AND EXISTS (
            SELECT 1 FROM UNNEST(t.region_stats) AS region WHERE region.region_code = "SE"
            )
            AND PARSE_DATE('%Y-%m-%d', (SELECT region.first_shown FROM UNNEST(t.region_stats) AS region WHERE region.region_code = "SE")) BETWEEN @start_date AND @end_date
    )
    SELECT 
        ads_with_dates.data_modified, 
        ads_with_dates.metadata_time, 
        ads_with_dates.advertiser_id, 
        ads_with_dates.creative_id, 
        ads_with_dates.raw_data
    FROM ads_with_dates
    WHERE NOT EXISTS (
        SELECT 1
        FROM `{project_id}.{dataset_id}.{table_id}` AS existing
        WHERE existing.advertiser_id = ads_with_dates.advertiser_id
        AND existing.creative_id = ads_with_dates.creative_id
        AND existing.raw_data = ads_with_dates.raw_data  
    )
"""

    query_params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
    ]

    if backfill:
        query_params.append(
            bigquery.ArrayQueryParameter("advertiser_ids", "STRING", advertiser_ids)
        )

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        query_job = bigquery_client.query(query, job_config=job_config)
        query_job.result()
        mode = "backfill" if backfill else "daily"
        logging.info(
            f"Successfully inserted/updated data for {mode} mode from {start_date} to {end_date}"
        )
        print(
            f"Successfully inserted/updated data for {mode} mode from {start_date} to {end_date}"
        )

    except Exception as e:
        logging.error(
            f"Failed to insert new data for range {start_date} to {end_date}: {e}"
        )
        print(
            f"Failed to insert new data for range {start_date} to {end_date}: {e}"
        )
        raise
