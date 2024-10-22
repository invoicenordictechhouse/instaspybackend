{{ config(
    materialized= 'incremental', 
    unique_key= ['advertiser_id', 'creative_id'],
    cluster_by = ['advertiser_id', 'creative_id']
) }}

WITH advertiser_ids AS (
    SELECT 
        advertiser_id
    FROM 
        {{ source('raw', 'advertisers_tracking') }}
    WHERE
        ('{{ var("advertiser_id", "NO_ID") }}' = "NO_ID" OR advertiser_id = '{{ var("advertiser_id") }}')
),

raw_data AS (
    SELECT 
        {{ extract_general_fields() }},
        {{ clean_json_string('raw_data') }} AS region_stats_json_cleaned,
        {{ extract_audience_fields() }},
        metadata_time  

    FROM  
        {{ source('raw', 'raw_sample_ads') }} 
    WHERE advertiser_id IN (
        SELECT advertiser_id FROM advertiser_ids
    )
    {% if is_incremental() %}
    AND (
        advertiser_id NOT IN (SELECT advertiser_id FROM {{ this }}) OR
        metadata_time > (SELECT MAX(metadata_time) FROM {{ this }})
    )
    {% endif %}
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
        ARRAY_AGG(STRUCT(
            JSON_EXTRACT_SCALAR(region, '$.region_code') AS region_code,
            JSON_EXTRACT_SCALAR(region, '$.first_shown') AS first_shown,
            JSON_EXTRACT_SCALAR(region, '$.last_shown') AS last_shown,
            JSON_EXTRACT_SCALAR(region, '$.times_shown_start_date') AS times_shown_start_date,
            JSON_EXTRACT_SCALAR(region, '$.times_shown_end_date') AS times_shown_end_date,
            JSON_EXTRACT_SCALAR(region, '$.times_shown_lower_bound') AS times_shown_lower_bound,
            JSON_EXTRACT_SCALAR(region, '$.times_shown_upper_bound') AS times_shown_upper_bound,
            JSON_EXTRACT_SCALAR(region, '$.times_shown_availability_date') AS times_shown_availability_date,
            JSON_QUERY(region, '$.surface_serving_stats') AS surface_serving_stats_raw
        )) AS region_data,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest

    FROM raw_data,
    UNNEST(JSON_EXTRACT_ARRAY(region_stats_json_cleaned, '$')) AS region
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

parsed_surface_serving AS (
    SELECT
        pr.*,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(TO_JSON_STRING(r.surface_serving_stats_raw), '^"+|"+$', ''), 
                '\\\\',
                ''
            ),
            '"{2,}', 
            '"'
        ) AS surface_serving_stats_cleaned
    FROM processed_regions pr,
    UNNEST(pr.region_data) AS r 
),

parsed_surface_serving_json AS (
    SELECT
        * EXCEPT(surface_serving_stats_cleaned),
        SAFE.PARSE_JSON(surface_serving_stats_cleaned) AS surface_serving_stats_json
    FROM parsed_surface_serving
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
        ARRAY_AGG(STRUCT(
            r.region_code,
            r.first_shown,
            r.last_shown,
            r.times_shown_start_date,
            r.times_shown_end_date,
            r.times_shown_lower_bound,
            r.times_shown_upper_bound,
            r.times_shown_availability_date,
            ARRAY(
                SELECT AS STRUCT
                    JSON_VALUE(surf, '$.surface') AS surface,
                    CAST(JSON_VALUE(surf, '$.times_shown_upper_bound') AS INT64) AS times_shown_upper_bound,
                    CAST(JSON_VALUE(surf, '$.times_shown_lower_bound') AS INT64) AS times_shown_lower_bound,
                    NULLIF(JSON_VALUE(surf, '$.times_shown_availability_date'), 'null') AS times_shown_availability_date
                FROM UNNEST(JSON_EXTRACT_ARRAY(surface_serving_stats_json, '$.surface_serving_stats')) AS surf
                ORDER BY CAST(JSON_VALUE(surf, '$.times_shown_upper_bound') AS INT64) DESC
            ) AS surface_serving_stats 
        )) AS region_surface_data,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest  
    FROM parsed_surface_serving_json pr,
    UNNEST(pr.region_data) AS r 
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

flattened_regions AS (
    SELECT
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_location,
        topic,
        is_funded_by_google_ad_grants,
        region.region_code,
        region.first_shown,
        region.last_shown,
        region.times_shown_start_date,
        region.times_shown_end_date,
        region.times_shown_lower_bound AS region_times_shown_lower_bound,
        region.times_shown_upper_bound AS region_times_shown_upper_bound,
        region.times_shown_availability_date AS region_times_shown_availability_date,  -- Rename here to avoid ambiguity
        region.surface_serving_stats,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest
    FROM structured_surface_serving,
    UNNEST(region_surface_data) AS region
),

flattened_surfaces AS (
    SELECT
        fr.advertiser_id,
        fr.creative_id,
        fr.creative_page_url,
        fr.ad_format_type,
        fr.advertiser_disclosed_name,
        fr.advertiser_location,
        fr.topic,
        fr.is_funded_by_google_ad_grants,
        fr.region_code,
        fr.first_shown,
        fr.last_shown,
        fr.times_shown_start_date,
        fr.times_shown_end_date,
        fr.region_times_shown_availability_date as times_shown_availability_date ,  -- Carry forward from flattened_regions
        s.surface AS surface,
        COALESCE(s.times_shown_lower_bound, 0) AS surface_times_shown_lower_bound,  -- Handle nulls
        COALESCE(s.times_shown_upper_bound, 0) AS surface_times_shown_upper_bound,  -- Handle nulls
        fr.demographic_info,
        fr.geo_location,
        fr.contextual_signals,
        fr.customer_lists,
        fr.topics_of_interest
    FROM flattened_regions fr
    LEFT JOIN UNNEST(fr.surface_serving_stats) AS s ON TRUE  -- Use LEFT JOIN to handle empty arrays
),

final_selection AS (
    SELECT
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_location,
        topic,
        is_funded_by_google_ad_grants,
        region_code,
        first_shown,
        last_shown,
        times_shown_start_date,
        times_shown_end_date,
        times_shown_availability_date,

        -- Use macro for surface-specific aggregation
        {{ aggregate_surface('YOUTUBE', 'surface_times_shown_lower_bound', 'surface_times_shown_upper_bound') }},
        {{ aggregate_surface('SEARCH', 'surface_times_shown_lower_bound', 'surface_times_shown_upper_bound') }},
        {{ aggregate_surface('SHOPPING', 'surface_times_shown_lower_bound', 'surface_times_shown_upper_bound') }},
        {{ aggregate_surface('MAPS', 'surface_times_shown_lower_bound', 'surface_times_shown_upper_bound') }},
        {{ aggregate_surface('PLAY', 'surface_times_shown_lower_bound', 'surface_times_shown_upper_bound') }},
        
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest
    FROM flattened_surfaces
    GROUP BY
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_location,
        topic,
        is_funded_by_google_ad_grants,
        region_code,
        first_shown,
        last_shown,
        times_shown_start_date,
        times_shown_end_date,
        times_shown_availability_date,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest
    ORDER BY
        last_shown DESC
)

SELECT *
FROM final_selection