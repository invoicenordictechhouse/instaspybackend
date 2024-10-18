from google.cloud import bigquery
from typing import List, Dict, Any
import logging

# Initialize BigQuery client
client = bigquery.Client()
logger = logging.getLogger(__name__)


def get_rows_from_bq() -> List[Dict[str, Any]]:
    """
    Queries BigQuery to retrieve up to 500 rows from the `dbt.testConsumption` table.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a row of data from the query result.

    Raises:
        Exception: Logs and returns an empty list if an error occurs during the query.
    """
    query = """
        SELECT * 
        FROM `dbt.testConsumption`
        LIMIT 500
    """
    try:
        logger.info("Starting BigQuery query to fetch rows from `dbt.testConsumption`.")
        # Execute the query
        query_job = client.query(query)
        results = query_job.result()

        # Convert the results to a list of dictionaries
        rows = [dict(row) for row in results]
        logger.info(f"Successfully fetched {len(rows)} rows from BigQuery.")
        return rows

    except Exception as e:
        logger.error(f"Failed to fetch rows from BigQuery: {e}")
        return []
