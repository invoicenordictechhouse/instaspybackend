-- TODO: Define marts model for aggregated Meta data

WITH clean_meta_data AS (
    SELECT * FROM {{ ref('stg_meta_raw') }}
)