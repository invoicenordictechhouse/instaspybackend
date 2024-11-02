{{ config(
    partition_by={
        'field': 'last_shown',
        'data_type': 'date',
    }
) }}


-- Step 1: Fetch advertiser IDs based on the provided advertiser ID or include all if no ID is given
WITH advertiser_ids AS (
    SELECT 
        advertiser_id
    FROM 
        {{ source('raw', 'advertisers_tracking_dev') }}
    WHERE
        ('{{ var("advertiser_id", "NO_ID") }}' = "NO_ID" OR advertiser_id = '{{ var("advertiser_id") }}')
),

-- Step 2: Pull raw ad data for relevant advertisers
raw_data AS (
    SELECT 
        -- Metadata for incremental updates
        metadata_time,  
        -- Macro to extract general ad fields (like IDs, names, etc.)
        {{ extract_general_fields() }},
        -- Clean JSON format for region stats
        {{ clean_json_string('raw_data') }} AS region_stats_json_cleaned,
        -- Macro to extract audience info from JSON
        {{ extract_audience_fields() }}

    FROM  
        {{ source('raw', 'raw_google_ads_dev') }} 
    WHERE advertiser_id IN (
        SELECT advertiser_id FROM advertiser_ids
    )
    {% if is_incremental() %}
    AND (
        -- Check if new ad
        advertiser_id NOT IN (SELECT advertiser_id FROM {{ this }}) OR
        -- Incremental update if data is newer
        metadata_time > (SELECT MAX(metadata_time) FROM {{ this }})
    )
    {% endif %}
),

-- Step 3: Process regions data, aggregating relevant region stats
processed_regions AS (
    SELECT
        metadata_time,
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
        -- Aggregate region-specific dat
        ARRAY_AGG(STRUCT(
            JSON_EXTRACT_SCALAR(region, '$.region_code') AS region_code,
            SAFE_CAST(JSON_EXTRACT_SCALAR(region, '$.first_shown') AS DATE) AS first_shown,
            SAFE_CAST(JSON_EXTRACT_SCALAR(region, '$.last_shown') AS DATE) AS last_shown,
            SAFE_CAST(JSON_EXTRACT_SCALAR(region, '$.times_shown_start_date') AS DATE) AS times_shown_start_date,
            SAFE_CAST(JSON_EXTRACT_SCALAR(region, '$.times_shown_end_date') AS DATE) AS times_shown_end_date,
            JSON_EXTRACT_SCALAR(region, '$.times_shown_lower_bound') AS times_shown_lower_bound,
            JSON_EXTRACT_SCALAR(region, '$.times_shown_upper_bound') AS times_shown_upper_bound,
            SAFE_CAST(JSON_EXTRACT_SCALAR(region, '$.times_shown_availability_date') AS DATE) AS times_shown_availability_date,
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
        metadata_time,
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

-- Step 4: Clean surface-serving stats from regions
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

-- Step 5: Convert the cleaned surface stats back to JSON
parsed_surface_serving_json AS (
    SELECT
        * EXCEPT(surface_serving_stats_cleaned),
        SAFE.PARSE_JSON(surface_serving_stats_cleaned) AS surface_serving_stats_json
    FROM parsed_surface_serving
),

-- Step 6: Aggregate region and surface data for each ad
structured_surface_serving AS (
    SELECT
        metadata_time,
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
        metadata_time,
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

-- Step 7: Flatten regions data
flattened_regions AS (
    SELECT
        metadata_time,
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

-- Step 8: Flatten surface data
flattened_surfaces AS (
    SELECT
        fr.metadata_time,
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
        -- Ensure availability date is preserved
        fr.region_times_shown_availability_date as times_shown_availability_date ,  
        s.surface AS surface,
        COALESCE(s.times_shown_lower_bound, 0) AS surface_times_shown_lower_bound, 
        COALESCE(s.times_shown_upper_bound, 0) AS surface_times_shown_upper_bound, 
        fr.demographic_info,
        fr.geo_location,
        fr.contextual_signals,
        fr.customer_lists,
        fr.topics_of_interest
    FROM flattened_regions fr
    -- Handle cases with empty arrays
    LEFT JOIN UNNEST(fr.surface_serving_stats) AS s ON TRUE  
),

-- Step 9: Final selection with aggregation for each surface type
final_selection AS (
    SELECT
        metadata_time,
        -- Add cleaned_at timestamp to indicate when the cleaning happened
        CURRENT_TIMESTAMP() AS cleaned_at,
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
        metadata_time,
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
)

-- Step 10: Output the final selection
SELECT * FROM final_selection