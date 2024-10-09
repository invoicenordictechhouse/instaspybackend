{{ config(
    materialized= 'incremental', 
    unique_key= ['advertiser_id', 'creative_id'],
    cluster_by = ['advertiser_id', 'creative_id']

) }}

WITH raw_data AS (
    SELECT 
        {{ extract_and_cast('raw_data', '$.advertiser_id', 'STRING') }} AS advertiser_id,
        {{ extract_and_cast('raw_data', '$.creative_id', 'STRING') }} AS creative_id,
        {{ extract_and_cast('raw_data', '$.creative_page_url', 'STRING') }} AS creative_page_url,
        {{ extract_and_cast('raw_data', '$.ad_format_type', 'STRING') }} AS ad_format_type,
        {{ extract_and_cast('raw_data', '$.advertiser_disclosed_name', 'STRING') }} AS advertiser_disclosed_name,
        {{ extract_and_cast('raw_data', '$.advertiser_legal_name', 'STRING') }} AS advertiser_legal_name,
        {{ extract_and_cast('raw_data', '$.advertiser_location', 'STRING') }} AS advertiser_location,
        {{ extract_and_cast('raw_data', '$.advertiser_verification_status', 'STRING') }} AS advertiser_verification_status,
        {{ extract_and_cast('raw_data', '$.topic', 'STRING') }} AS topic,
        {{ extract_and_cast('raw_data', '$.is_funded_by_google_ad_grants', 'BOOL') }} AS is_funded_by_google_ad_grants,
        {{ clean_json_string('raw_data') }} AS region_stats_json_cleaned,
        SAFE.PARSE_JSON(JSON_EXTRACT_SCALAR(raw_data, '$.audience_selection_approach_info_json')) AS audience_selection_info,
        SAFE_CAST(JSON_VALUE(SAFE.PARSE_JSON(JSON_EXTRACT_SCALAR(raw_data, '$.audience_selection_approach_info_json')), '$.demographic_info') AS STRING) AS demographic_info,
        SAFE_CAST(JSON_VALUE(SAFE.PARSE_JSON(JSON_EXTRACT_SCALAR(raw_data, '$.audience_selection_approach_info_json')), '$.geo_location') AS STRING) AS geo_location,
        SAFE_CAST(JSON_VALUE(SAFE.PARSE_JSON(JSON_EXTRACT_SCALAR(raw_data, '$.audience_selection_approach_info_json')), '$.contextual_signals') AS STRING) AS contextual_signals,
        SAFE_CAST(JSON_VALUE(SAFE.PARSE_JSON(JSON_EXTRACT_SCALAR(raw_data, '$.audience_selection_approach_info_json')), '$.customer_lists') AS STRING) AS customer_lists,
        SAFE_CAST(JSON_VALUE(SAFE.PARSE_JSON(JSON_EXTRACT_SCALAR(raw_data, '$.audience_selection_approach_info_json')), '$.topics_of_interest') AS STRING) AS topics_of_interest,
        metadata_time  

    FROM  
        {{ source('raw', 'raw_google_ads') }} 
    {% if is_incremental() %}
    WHERE metadata_time  > (SELECT MAX(metadata_time ) FROM {{ this }})
    {% endif %}
),

parsed_data AS (
    SELECT
        *,
        SAFE.PARSE_JSON(region_stats_json_cleaned) AS region_stats_json
    FROM raw_data
),

region_array AS (
    SELECT
        pd.* EXCEPT(region_stats_json_cleaned, region_stats_json),
        CASE
            WHEN JSON_TYPE(pd.region_stats_json) = 'ARRAY' THEN
                (SELECT ARRAY_AGG(SAFE.PARSE_JSON(elem))
                 FROM UNNEST(JSON_QUERY_ARRAY(TO_JSON_STRING(pd.region_stats_json))) AS elem)
            ELSE
                [pd.region_stats_json]
        END AS regions
    FROM parsed_data pd
),

unnested_regions AS (
    SELECT
        ra.*,
        region
    FROM region_array ra,
    UNNEST(ra.regions) AS region
),

processed_regions AS (
    SELECT
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_legal_name,
        advertiser_location,
        advertiser_verification_status,
        topic,
        is_funded_by_google_ad_grants,
        STRUCT(
            SAFE_CAST(JSON_VALUE(region, '$.region_code') AS STRING) AS region_code,
            SAFE_CAST(JSON_VALUE(region, '$.first_shown') AS DATE) AS first_shown,
            SAFE_CAST(JSON_VALUE(region, '$.last_shown') AS DATE) AS last_shown,
            SAFE_CAST(JSON_VALUE(region, '$.times_shown_start_date') AS DATE) AS times_shown_start_date,
            SAFE_CAST(JSON_VALUE(region, '$.times_shown_end_date') AS DATE) AS times_shown_end_date,
            SAFE_CAST(JSON_VALUE(region, '$.times_shown_lower_bound') AS INT64) AS times_shown_lower_bound,
            SAFE_CAST(JSON_VALUE(region, '$.times_shown_upper_bound') AS INT64) AS times_shown_upper_bound,
            SAFE_CAST(JSON_VALUE(region, '$.times_shown_availability_date') AS DATE) AS times_shown_availability_date,
            JSON_QUERY(region, '$.surface_serving_stats') AS surface_serving_stats_raw
          
        ) AS region_struct,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest
    FROM unnested_regions
),

parsed_surface_serving AS (
    SELECT
        *,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(TO_JSON_STRING(region_struct.surface_serving_stats_raw), '^"+|"+$', ''),
                '\\\\',
                ''
            ),
            '"{2,}',
            '"'
        ) AS surface_serving_stats_cleaned
    FROM processed_regions
),

parsed_surface_serving_json AS (
    SELECT
        *,
        SAFE.PARSE_JSON(surface_serving_stats_cleaned) AS surface_serving_stats_json
    FROM parsed_surface_serving
),

extracted_surface_serving AS (
    SELECT
        *,
        JSON_QUERY_ARRAY(surface_serving_stats_json, '$.surface_serving_stats') AS surface_serving_stats_array
    FROM parsed_surface_serving_json
),

structured_surface_serving AS (
    SELECT
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_legal_name,
        advertiser_location,
        advertiser_verification_status,
        topic,
        is_funded_by_google_ad_grants,
        region_struct.region_code AS region_code,
        region_struct.first_shown AS first_shown,
        region_struct.last_shown AS last_shown,
        region_struct.times_shown_start_date AS times_shown_start_date,
        region_struct.times_shown_end_date AS times_shown_end_date,
        region_struct.times_shown_lower_bound AS times_shown_lower_bound,
        region_struct.times_shown_upper_bound AS times_shown_upper_bound,
        region_struct.times_shown_availability_date AS times_shown_availability_date,

        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(surf, '$.surface') AS surface,
                CAST(JSON_VALUE(surf, '$.times_shown_upper_bound') AS INT64) AS times_shown_upper_bound,
                CAST(JSON_VALUE(surf, '$.times_shown_lower_bound') AS INT64) AS times_shown_lower_bound,
                NULLIF(JSON_VALUE(surf, '$.times_shown_availability_date'), 'null') AS times_shown_availability_date
            FROM UNNEST(surface_serving_stats_array) AS surf
        ) AS surface_serving_stats,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest
    FROM extracted_surface_serving
),

aggregated_data AS (
    SELECT
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_legal_name,
        advertiser_location,
        advertiser_verification_status,
        topic,
        is_funded_by_google_ad_grants,

        ARRAY_AGG(STRUCT(
            region_code,
            first_shown,
            last_shown,
            times_shown_start_date,
            times_shown_end_date,
            times_shown_lower_bound,
            times_shown_upper_bound,
            times_shown_availability_date,
            surface_serving_stats
        )) AS region_stats,
         COUNTIF(
            (times_shown_availability_date IS NULL OR times_shown_availability_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
            AND 
            (times_shown_end_date IS NULL OR times_shown_end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        ) AS active_count,
        COUNTIF(last_shown > times_shown_end_date) AS anomaly_count,  -- New anomaly count
        MAX(last_shown) AS max_last_time_shown,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest
        
    FROM structured_surface_serving
    GROUP BY
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_legal_name,
        advertiser_location,
        advertiser_verification_status,
        topic,
        is_funded_by_google_ad_grants,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest
),

final_selection AS (
    SELECT
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_legal_name,
        advertiser_location,
        advertiser_verification_status,
        topic,
        CASE
            WHEN active_count > 0 THEN TRUE
            ELSE FALSE
        END AS is_active,
        CASE
            WHEN anomaly_count > 0 THEN TRUE
            ELSE FALSE
        END AS anomaly,
        is_funded_by_google_ad_grants,
        region_stats,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest
    FROM aggregated_data
)

SELECT
    *
FROM final_selection