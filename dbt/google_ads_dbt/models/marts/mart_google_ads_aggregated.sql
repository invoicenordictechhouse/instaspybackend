-- TODO: Define marts model for aggregated google ads data

WITH clean_google_ads_data AS (
    SELECT * FROM {{ ref('stg_meta_raw') }}
)