WITH base AS (
    SELECT
        metadata_time,
        cleaned_at,
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_location,
        topic,
        region_code,
        first_shown,
        last_shown,
        times_shown_start_date,
        times_shown_end_date,
        times_shown_availability_date,
        youtube_times_shown_lower_bound,
        youtube_times_shown_upper_bound,
        search_times_shown_lower_bound,
        search_times_shown_upper_bound,
        shopping_times_shown_lower_bound,
        shopping_times_shown_upper_bound,
        maps_times_shown_lower_bound,
        maps_times_shown_upper_bound,
        play_times_shown_lower_bound,
        play_times_shown_upper_bound,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest
    FROM
        {{ source('clean', 'clean_google_ads') }} 
    {% if is_incremental() %}
    WHERE metadata_time > (SELECT MAX(metadata_time) FROM {{ this }})
    {% endif %}
),

ad_metrics_and_flags AS (
    SELECT
        metadata_time,
        cleaned_at,
        advertiser_id,
        creative_id,
        creative_page_url,
        ad_format_type,
        advertiser_disclosed_name,
        advertiser_location,
        topic,
        region_code,
        first_shown,
        last_shown,
        times_shown_start_date,
        times_shown_end_date,
        youtube_times_shown_lower_bound,
        youtube_times_shown_upper_bound,
        search_times_shown_lower_bound,
        search_times_shown_upper_bound,
        shopping_times_shown_lower_bound,
        shopping_times_shown_upper_bound,
        maps_times_shown_lower_bound,
        maps_times_shown_upper_bound,
        play_times_shown_lower_bound,
        play_times_shown_upper_bound,
        demographic_info,
        geo_location,
        contextual_signals,
        customer_lists,
        topics_of_interest,

        -- Flag for active status, considering one-day lag
        CASE
            WHEN CAST(last_shown AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) THEN TRUE
            ELSE FALSE
        END AS is_active,

        -- Flag for data availability
        CASE
            WHEN times_shown_availability_date IS NULL THEN FALSE
            ELSE TRUE
        END AS is_data_unavailable,

        -- Calculate days since first shown and last shown based on yesterday
        DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), CAST(first_shown AS DATE), DAY) AS days_since_first_shown,
        DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), CAST(last_shown AS DATE), DAY) AS days_since_last_shown,

        -- Days active based on times_shown_start_date and times_shown_end_date (for reporting period)
        CASE 
            WHEN times_shown_start_date IS NOT NULL AND times_shown_end_date IS NOT NULL THEN
                DATE_DIFF(CAST(times_shown_end_date AS DATE), CAST(times_shown_start_date AS DATE), DAY)
            ELSE NULL
        END AS days_active_impression_data,

        -- Days active based on first_shown and last_shown (overall visibility)
        CASE 
            WHEN first_shown IS NOT NULL AND last_shown IS NOT NULL THEN
                DATE_DIFF(CAST(last_shown AS DATE), CAST(first_shown AS DATE), DAY)
            ELSE NULL
        END AS days_active_visibility,

        -- Availability countdown
        COALESCE(DATE_DIFF(CAST(times_shown_availability_date AS DATE), CURRENT_DATE(), DAY), 0) AS availability_window,

        -- Total impressions as sum of upper bounds from all platforms
        SAFE_CAST(
        COALESCE(SAFE_CAST(youtube_times_shown_upper_bound AS NUMERIC), 0) +
        COALESCE(SAFE_CAST(search_times_shown_upper_bound AS NUMERIC), 0) +
        COALESCE(SAFE_CAST(shopping_times_shown_upper_bound AS NUMERIC), 0) +
        COALESCE(SAFE_CAST(maps_times_shown_upper_bound AS NUMERIC), 0) +
        COALESCE(SAFE_CAST(play_times_shown_upper_bound AS NUMERIC), 0)
        AS NUMERIC) AS total_impressions
    FROM base
)

SELECT * FROM ad_metrics_and_flags
