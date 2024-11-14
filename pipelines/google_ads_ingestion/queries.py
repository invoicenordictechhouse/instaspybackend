
CHECK_ROW_COUNT_QUERY = """
SELECT COUNT(*) as row_count FROM `{project_id}.{dataset_id}.{table_id}`
"""

ADVERTISER_IDS_SUBQUERY = """
SELECT x AS advertiser_id FROM UNNEST(@advertiser_ids) AS x
"""

ADVERTISER_TRACKING_SUBQUERY = """
SELECT advertiser_id FROM `{project_id}.{dataset_id}.{advertiser_ids_table}`
"""

CHECK_DATA_AVAILABILITY_QUERY = """
SELECT 1
FROM `bigquery-public-data.google_ads_transparency_center.creative_stats` AS t
WHERE
    t.advertiser_id IN ({advertiser_ids_subquery})
    AND t.advertiser_location = "SE"
    AND EXISTS (
        SELECT 1 FROM UNNEST(t.region_stats) AS region WHERE region.region_code = "SE"
    )
    AND PARSE_DATE('%Y-%m-%d', (SELECT region.first_shown FROM UNNEST(t.region_stats) AS region WHERE region.region_code = "SE"))
        BETWEEN @start_date AND @end_date
LIMIT 1
"""

INSERT_NEW_GOOGLE_ADS_DATA_QUERY = """
INSERT INTO `{project_id}.{dataset_id}.{table_id}`
(data_modified, metadata_time, advertiser_id, creative_id, raw_data)

WITH selected_advertisers AS (
    {selected_advertisers_query}
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
        AND PARSE_DATE('%Y-%m-%d', (SELECT region.last_shown FROM UNNEST(t.region_stats) AS region WHERE region.region_code = "SE")) BETWEEN @start_date AND @end_date
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

ADD_UPDATED_ADS_QUERY = """
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
        t.advertiser_id IN (SELECT advertiser_id FROM `{project_id}.{dataset_id}.{advertisers_tracking_table_id}`)
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
    WHERE existing.advertiser_id = filtered_ads.advertiser_id
    AND existing.creative_id = filtered_ads.creative_id
    AND existing.raw_data = filtered_ads.raw_data
)
"""

ADD_TARGETED_ADS_QUERY = """
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
    WHERE existing.advertiser_id = filtered_ads.advertiser_id
    AND existing.creative_id = filtered_ads.creative_id
    AND existing.raw_data = filtered_ads.raw_data
)
"""