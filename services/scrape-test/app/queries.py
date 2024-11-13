# queries.py

"""
Module containing SQL queries used in the application.
"""

GET_ROWS_QUERY: str = """
SELECT
    advertiser_id,
    creative_id,
    creative_page_url
FROM
    `{table}`
WHERE
    advertiser_id = @advertiser_id
    AND advertiser_id IS NOT NULL
    AND creative_id IS NOT NULL
    AND creative_page_url IS NOT NULL
"""
"""
SQL query to retrieve rows for a specific advertiser ID.

Parameters:
    table (str): The fully qualified BigQuery table name.
"""
